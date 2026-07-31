#include "peel_transport.h"

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <map>
#include <netinet/ip.h>
#include <netinet/udp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unordered_map>
#include <unordered_set>

namespace gloo {
namespace transport {
namespace peel {

namespace {

using Clock = std::chrono::steady_clock;

const uint8_t* parseRenoUdpFrame(
    const uint8_t* frame,
    ssize_t n,
    uint32_t filter_dst_ip_n,
    uint16_t filter_dst_port_h,
    uint32_t& src_ip_n,
    uint16_t& src_port_h,
    size_t& payload_len) {
    if (n < static_cast<ssize_t>(14 + 20 + 8)) {
        return nullptr;
    }
    if (((frame[12] << 8) | frame[13]) != 0x0800) {
        return nullptr;
    }

    const auto* ip = reinterpret_cast<const iphdr*>(frame + 14);
    const int ip_header_len = ip->ihl * 4;
    if (ip->version != 4 || ip_header_len < 20 ||
        ip->protocol != IPPROTO_UDP ||
        (filter_dst_ip_n != 0 && ip->daddr != filter_dst_ip_n)) {
        return nullptr;
    }

    const uint16_t ip_total_len = ntohs(ip->tot_len);
    if (static_cast<size_t>(14) + ip_total_len > static_cast<size_t>(n) ||
        static_cast<size_t>(14 + ip_header_len + 8) >
            static_cast<size_t>(n)) {
        return nullptr;
    }

    const auto* udp =
        reinterpret_cast<const udphdr*>(frame + 14 + ip_header_len);
    if (filter_dst_port_h != 0 &&
        ntohs(udp->dest) != filter_dst_port_h) {
        return nullptr;
    }

    const uint16_t udp_len = ntohs(udp->len);
    if (udp_len < 8 ||
        static_cast<size_t>(14 + ip_header_len) + udp_len >
            static_cast<size_t>(n)) {
        return nullptr;
    }

    src_ip_n = ip->saddr;
    src_port_h = ntohs(udp->source);
    payload_len = udp_len - 8;
    return reinterpret_cast<const uint8_t*>(udp) + 8;
}

uint64_t packPeerKey(uint32_t ip_n, uint16_t port_h) {
    return (static_cast<uint64_t>(ip_n) << 16) | port_h;
}

enum class RenoCCState {
    SlowStart,
    CongestionAvoidance,
    FastRecovery,
};

struct RenoStateMachine {
    RenoStateMachine(double initial_cwnd, double initial_ssthresh)
        : cwnd(std::max(1.0, initial_cwnd)),
          ssthresh(std::max(2.0, initial_ssthresh)) {}

    uint32_t windowSize() const {
        return static_cast<uint32_t>(std::max(1.0, cwnd));
    }

    void onFirstAck(uint32_t count) {
        switch (state) {
            case RenoCCState::SlowStart:
                cwnd += static_cast<double>(count);
                if (cwnd >= ssthresh) {
                    state = RenoCCState::CongestionAvoidance;
                }
                break;
            case RenoCCState::CongestionAvoidance:
                cwnd += static_cast<double>(count) / cwnd;
                break;
            case RenoCCState::FastRecovery:
                cwnd += static_cast<double>(count);
                break;
        }
    }

    void onDuplicateAckThreshold(uint32_t lost_seq) {
        if (state != RenoCCState::FastRecovery) {
            ssthresh = std::max(cwnd / 2.0, 2.0);
            cwnd = ssthresh + 3.0;
            state = RenoCCState::FastRecovery;
        }
        recovery_point = std::max(recovery_point, lost_seq);
    }

    void onRecoveryAck() {
        cwnd = ssthresh;
        state = RenoCCState::CongestionAvoidance;
    }

    void onPartialAck(uint32_t newly_acked) {
        cwnd =
            std::max(1.0, cwnd - static_cast<double>(newly_acked) + 1.0);
    }

    void onTimeout() {
        ssthresh = std::max(cwnd / 2.0, 2.0);
        cwnd = 1.0;
        state = RenoCCState::SlowStart;
    }

    double cwnd;
    double ssthresh;
    RenoCCState state = RenoCCState::SlowStart;
    uint32_t recovery_point = 0;
};

struct RenoAckSlot {
    std::unordered_set<uint64_t> ack_senders;
    bool first_ack_done = false;

    std::unordered_set<uint64_t> dup_ack_senders;
    Clock::time_point dupack_window_start{};
    bool dupack_window_open = false;

    uint8_t retrans_id = 1;
    bool is_retransmit = false;
};

struct RenoAckWindow {
    void add(uint32_t seq) {
        slots.emplace(seq, RenoAckSlot{});
    }

    void markRetransmit(uint32_t seq, uint8_t retrans_id) {
        auto it = slots.find(seq);
        if (it == slots.end()) {
            return;
        }
        auto& slot = it->second;
        slot.retrans_id = retrans_id;
        slot.is_retransmit = true;
        slot.first_ack_done = true;
        slot.dup_ack_senders.clear();
        slot.dupack_window_open = false;

        // Preserve ack_senders. A multicast retransmission may be intended for
        // only one lagging receiver; peers that already advanced their
        // cumulative ACK must not be forced to acknowledge the slot again.
    }

    void eraseBelow(uint32_t seq) {
        for (auto it = slots.begin(); it != slots.end();) {
            it = it->first < seq ? slots.erase(it) : std::next(it);
        }
    }

    RenoAckSlot* get(uint32_t seq) {
        auto it = slots.find(seq);
        return it == slots.end() ? nullptr : &it->second;
    }

    const RenoAckSlot* get(uint32_t seq) const {
        auto it = slots.find(seq);
        return it == slots.end() ? nullptr : &it->second;
    }

    std::unordered_map<uint32_t, RenoAckSlot> slots;
};

struct RenoAckAggregator {
    std::mutex mutex;
    std::condition_variable cv;

    std::unordered_map<uint64_t, uint32_t> peer_cum_ack;
    std::unordered_map<uint64_t, uint32_t> peer_rwnd;
    RenoAckWindow ack_window;

    uint32_t committed_una = 1;
    uint32_t first_ack_count = 0;
    bool fast_retransmit_needed = false;
    uint32_t fast_retransmit_seq = 0;
    uint32_t last_tsecr = 0;
    bool tsecr_valid = false;
    uint32_t min_rwnd = 0xffff;
};

struct RenoInFlight {
    size_t offset = 0;
    size_t length = 0;
    Clock::time_point sent_at{};
    uint32_t tsval = 0;
    uint8_t retrans_id = 1;
};

struct RenoOutOfOrder {
    std::vector<uint8_t> payload;
    uint32_t tsval = 0;
    uint8_t retrans_id = 1;
};

void updateRenoRtt(
    double& srtt,
    double& rttvar,
    int& rto_ms,
    double sample_ms) {
    if (srtt < 0.0) {
        srtt = sample_ms;
        rttvar = sample_ms / 2.0;
    } else {
        const double error = sample_ms - srtt;
        rttvar = 0.75 * rttvar + 0.25 * std::abs(error);
        srtt = 0.875 * srtt + 0.125 * sample_ms;
    }
    const int computed = static_cast<int>(srtt + 4.0 * rttvar);
    rto_ms = std::min(std::max(computed, 10), 30000);
}

} // namespace

bool PeelTransport::sendReno(const void* data, size_t size) {
    if (!isReady() || !mesh_result_->send_channel ||
        (data == nullptr && size != 0)) {
        return false;
    }
    if (size == 0) {
        return true;
    }

    auto* channel = mesh_result_->send_channel.get();
    const auto* bytes = static_cast<const uint8_t*>(data);
    const size_t chunk_size = config_.max_chunk_size;
    if (chunk_size == 0) {
        return false;
    }

    const size_t segment_count_size = (size + chunk_size - 1) / chunk_size;
    if (segment_count_size == 0 ||
        segment_count_size >
            static_cast<size_t>(
                std::numeric_limits<uint32_t>::max() - next_seq_ - 1)) {
        std::cerr << "peel_reno[" << config_.rank
                  << "]: transfer is too large for the sequence space\n";
        return false;
    }

    const uint32_t first_seq = next_seq_;
    const uint32_t segment_count =
        static_cast<uint32_t>(segment_count_size);
    const uint32_t data_end_seq = first_seq + segment_count;
    const uint32_t fin_seq = data_end_seq;

    std::unordered_set<uint64_t> cohort;
    size_t expected_receivers = 0;
    auto add_peer = [&](int rank) {
        if (rank == config_.rank) {
            return;
        }
        ++expected_receivers;
        if (rank < 0 || rank >= config_.world_size ||
            static_cast<size_t>(rank) >= mesh_result_->peers.size()) {
            return;
        }
        const auto& peer = mesh_result_->peers[rank];
        if (peer.sin_addr.s_addr != 0 && peer.sin_port != 0) {
            cohort.insert(
                packPeerKey(peer.sin_addr.s_addr, ntohs(peer.sin_port)));
        }
    };

    if (config_.participant_ranks.empty()) {
        for (int rank = 0; rank < config_.world_size; ++rank) {
            add_peer(rank);
        }
    } else {
        for (int rank : config_.participant_ranks) {
            add_peer(rank);
        }
    }

    if (cohort.size() != expected_receivers) {
        std::cerr << "peel_reno[" << config_.rank << "]: discovered "
                  << cohort.size() << "/" << expected_receivers
                  << " receiver address(es)\n";
        return false;
    }
    if (cohort.empty()) {
        return true;
    }

    const uint16_t initial_window =
        static_cast<uint16_t>(std::min<uint32_t>(
            config_.reno_ooo_buffer_segments, 0xffffu));
    if (!sendPacket(
            first_seq,
            FLG_START,
            nullptr,
            0,
            1,
            peel_now_ms(),
            initial_window)) {
        return false;
    }

    RenoAckAggregator aggregator;
    aggregator.committed_una = first_seq;
    for (uint64_t peer : cohort) {
        aggregator.peer_cum_ack[peer] = first_seq;
        aggregator.peer_rwnd[peer] = 0xffff;
    }

    std::atomic<bool> aggregator_stop{false};
    const size_t fast_retransmit_threshold = std::max<size_t>(
        1,
        static_cast<size_t>(std::ceil(
            static_cast<double>(cohort.size()) *
            static_cast<double>(config_.reno_dupack_pct) / 100.0)));

    std::thread aggregator_thread([&] {
        while (!aggregator_stop.load()) {
            fd_set read_fds;
            FD_ZERO(&read_fds);
            FD_SET(channel->fd, &read_fds);
            timeval timeout{};
            timeout.tv_usec = 10000;
            if (select(
                    channel->fd + 1,
                    &read_fds,
                    nullptr,
                    nullptr,
                    &timeout) <= 0) {
                continue;
            }

            uint8_t frame[2048];
            const ssize_t n =
                ::recv(channel->fd, frame, sizeof(frame), 0);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK ||
                    errno == EINTR) {
                    continue;
                }
                break;
            }

            uint32_t src_ip = 0;
            uint16_t src_port = 0;
            size_t payload_len = 0;
            const uint8_t* payload = parseRenoUdpFrame(
                frame,
                n,
                mesh_result_->src_ip_n,
                channel->port,
                src_ip,
                src_port,
                payload_len);
            if (!payload || payload_len < PEEL_HEADER_SIZE) {
                continue;
            }

            PeelHeader header{};
            std::memcpy(&header, payload, sizeof(header));
            if (!peel_verify_header_checksum(header) ||
                (ntohs(header.flags) & FLG_ACK) == 0) {
                continue;
            }

            const uint64_t peer_key = packPeerKey(src_ip, src_port);
            if (cohort.count(peer_key) == 0) {
                continue;
            }

            const uint32_t cumulative_ack = ntohl(header.seq);
            if (cumulative_ack < first_seq ||
                cumulative_ack > data_end_seq) {
                continue;
            }
            const uint32_t tsecr = ntohl(header.tsecr);
            const uint8_t packet_retrans_id = header.retrans_id;

            std::lock_guard<std::mutex> lock(aggregator.mutex);
            auto peer_it = aggregator.peer_cum_ack.find(peer_key);
            if (peer_it == aggregator.peer_cum_ack.end()) {
                continue;
            }
            const uint32_t peer_previous = peer_it->second;

            const uint16_t advertised_window = ntohs(header.window);
            if (advertised_window > 0) {
                aggregator.peer_rwnd[peer_key] = advertised_window;
                uint32_t minimum_window = 0xffff;
                for (const auto& entry : aggregator.peer_rwnd) {
                    minimum_window =
                        std::min(minimum_window, entry.second);
                }
                if (minimum_window != aggregator.min_rwnd) {
                    aggregator.min_rwnd = minimum_window;
                    aggregator.cv.notify_all();
                }
            }

            if (cumulative_ack > peer_previous) {
                if (cumulative_ack > first_seq) {
                    const RenoAckSlot* last =
                        aggregator.ack_window.get(cumulative_ack - 1);
                    if (last &&
                        packet_retrans_id < last->retrans_id) {
                        continue;
                    }
                }

                uint32_t new_first_acks = 0;
                for (uint32_t seq = peer_previous;
                     seq < cumulative_ack;
                     ++seq) {
                    RenoAckSlot* slot =
                        aggregator.ack_window.get(seq);
                    if (!slot) {
                        continue;
                    }
                    slot->ack_senders.insert(peer_key);
                    if (!slot->first_ack_done) {
                        slot->first_ack_done = true;
                        ++new_first_acks;
                    }
                }
                aggregator.first_ack_count += new_first_acks;
                peer_it->second = cumulative_ack;

                uint32_t committed = aggregator.committed_una;
                while (true) {
                    const RenoAckSlot* slot =
                        aggregator.ack_window.get(committed);
                    if (!slot ||
                        slot->ack_senders.size() < cohort.size()) {
                        break;
                    }
                    ++committed;
                }
                const bool advanced =
                    committed > aggregator.committed_una;
                aggregator.committed_una = committed;

                const RenoAckSlot* last =
                    cumulative_ack > first_seq
                    ? aggregator.ack_window.get(cumulative_ack - 1)
                    : nullptr;
                if (last && !last->is_retransmit && tsecr != 0) {
                    aggregator.last_tsecr = tsecr;
                    aggregator.tsecr_valid = true;
                }

                if (new_first_acks > 0 || advanced) {
                    aggregator.cv.notify_all();
                }
                continue;
            }

            RenoAckSlot* slot =
                aggregator.ack_window.get(peer_previous);
            if (!slot || packet_retrans_id < slot->retrans_id) {
                continue;
            }

            const auto now = Clock::now();
            if (!slot->dupack_window_open) {
                slot->dupack_window_open = true;
                slot->dupack_window_start = now;
            } else if (
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - slot->dupack_window_start).count() >
                config_.reno_tagg_ms) {
                slot->dup_ack_senders.clear();
                slot->dupack_window_start = now;
            }

            if (slot->dup_ack_senders.insert(peer_key).second &&
                slot->dup_ack_senders.size() >=
                    fast_retransmit_threshold &&
                !aggregator.fast_retransmit_needed &&
                !slot->is_retransmit) {
                aggregator.fast_retransmit_needed = true;
                aggregator.fast_retransmit_seq = peer_previous;
                aggregator.cv.notify_all();
            }
        }
    });

    std::map<uint32_t, RenoInFlight> in_flight;
    uint32_t snd_nxt = first_seq;

    auto transmit_segment =
        [&](uint32_t seq, bool retransmit) -> bool {
        RenoInFlight meta{};
        auto existing = in_flight.find(seq);
        if (retransmit) {
            if (existing == in_flight.end()) {
                return true;
            }
            meta = existing->second;
            meta.retrans_id = static_cast<uint8_t>(
                std::min(
                    static_cast<int>(meta.retrans_id) + 1, 8));
        } else {
            meta.offset =
                static_cast<size_t>(seq - first_seq) * chunk_size;
            meta.length =
                std::min(chunk_size, size - meta.offset);
            meta.retrans_id = 1;
        }

        meta.tsval = peel_now_ms();
        if (!sendPacket(
                seq,
                FLG_DATA,
                bytes + meta.offset,
                meta.length,
                meta.retrans_id,
                meta.tsval,
                1)) {
            return false;
        }
        meta.sent_at = Clock::now();
        in_flight[seq] = meta;
        return true;
    };

    auto transfer = [&]() -> bool {
        uint32_t previous_committed = first_seq;
        int consecutive_timeouts = 0;
        double srtt = -1.0;
        double rttvar = 0.0;
        int rto_ms = config_.rto_ms;
        uint32_t last_known_min_rwnd = 0xffff;
        RenoStateMachine reno(
            config_.reno_initial_cwnd,
            config_.reno_initial_ssthresh);

        while (true) {
            uint32_t committed = first_seq;
            {
                std::lock_guard<std::mutex> lock(aggregator.mutex);
                committed = aggregator.committed_una;
            }
            if (committed >= data_end_seq) {
                return true;
            }

            const uint32_t effective_window = std::max<uint32_t>(
                1,
                std::min(
                    reno.windowSize(), last_known_min_rwnd));
            while (snd_nxt < data_end_seq &&
                   snd_nxt - committed < effective_window) {
                {
                    std::lock_guard<std::mutex> lock(
                        aggregator.mutex);
                    aggregator.ack_window.add(snd_nxt);
                }
                if (!transmit_segment(snd_nxt, false)) {
                    return false;
                }
                ++snd_nxt;
            }

            if (in_flight.empty()) {
                return false;
            }

            const auto expiry =
                in_flight.begin()->second.sent_at +
                std::chrono::milliseconds(rto_ms);
            auto wait_duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    expiry - Clock::now());
            if (wait_duration.count() < 0) {
                wait_duration = std::chrono::milliseconds(0);
            }

            uint32_t first_ack_events = 0;
            bool fast_retransmit = false;
            uint32_t fast_retransmit_seq = 0;
            uint32_t tsecr_sample = 0;
            bool have_tsecr = false;

            std::unique_lock<std::mutex> lock(aggregator.mutex);
            const bool woke = aggregator.cv.wait_for(
                lock,
                wait_duration,
                [&] {
                    return
                        aggregator.committed_una >
                            previous_committed ||
                        aggregator.first_ack_count > 0 ||
                        aggregator.fast_retransmit_needed ||
                        aggregator.min_rwnd !=
                            last_known_min_rwnd;
                });
            const uint32_t new_committed =
                aggregator.committed_una;
            const uint32_t current_min_rwnd =
                aggregator.min_rwnd;
            first_ack_events = aggregator.first_ack_count;
            aggregator.first_ack_count = 0;
            if (aggregator.fast_retransmit_needed) {
                fast_retransmit = true;
                fast_retransmit_seq =
                    aggregator.fast_retransmit_seq;
                aggregator.fast_retransmit_needed = false;
            }
            if (aggregator.tsecr_valid) {
                tsecr_sample = aggregator.last_tsecr;
                have_tsecr = true;
                aggregator.tsecr_valid = false;
            }
            lock.unlock();

            last_known_min_rwnd = current_min_rwnd;
            if (have_tsecr && tsecr_sample != 0) {
                const uint32_t now = peel_now_ms();
                if (now >= tsecr_sample) {
                    updateRenoRtt(
                        srtt,
                        rttvar,
                        rto_ms,
                        static_cast<double>(now - tsecr_sample));
                }
            }
            if (first_ack_events > 0) {
                reno.onFirstAck(first_ack_events);
            }

            if (woke && new_committed > previous_committed) {
                consecutive_timeouts = 0;
                for (auto it = in_flight.begin();
                     it != in_flight.end() &&
                         it->first < new_committed;) {
                    it = in_flight.erase(it);
                }
                {
                    std::lock_guard<std::mutex> ack_lock(
                        aggregator.mutex);
                    aggregator.ack_window.eraseBelow(
                        new_committed);
                }

                const uint32_t old_committed =
                    previous_committed;
                previous_committed = new_committed;
                if (config_.reno_rto_reset_on_ack) {
                    rto_ms = srtt > 0.0
                        ? std::min(
                              std::max(
                                  static_cast<int>(
                                      srtt + 4.0 * rttvar),
                                  10),
                              30000)
                        : config_.rto_ms;
                }

                if (reno.state == RenoCCState::FastRecovery &&
                    new_committed > reno.recovery_point) {
                    reno.onRecoveryAck();
                } else if (
                    reno.state == RenoCCState::FastRecovery &&
                    new_committed < data_end_seq) {
                    reno.onPartialAck(
                        new_committed - old_committed);
                    if (!transmit_segment(
                            new_committed, true)) {
                        return false;
                    }
                    auto it = in_flight.find(new_committed);
                    if (it != in_flight.end()) {
                        std::lock_guard<std::mutex> ack_lock(
                            aggregator.mutex);
                        aggregator.ack_window.markRetransmit(
                            new_committed,
                            it->second.retrans_id);
                    }
                }
            }

            if (fast_retransmit &&
                in_flight.count(fast_retransmit_seq) != 0) {
                reno.onDuplicateAckThreshold(
                    fast_retransmit_seq);
                if (!transmit_segment(
                        fast_retransmit_seq, true)) {
                    return false;
                }
                const auto it =
                    in_flight.find(fast_retransmit_seq);
                if (it != in_flight.end()) {
                    std::lock_guard<std::mutex> ack_lock(
                        aggregator.mutex);
                    aggregator.ack_window.markRetransmit(
                        fast_retransmit_seq,
                        it->second.retrans_id);
                }
            }

            if (!woke) {
                if (++consecutive_timeouts >
                    PEEL_DEFAULT_RETRIES) {
                    std::cerr
                        << "peel_reno[" << config_.rank
                        << "]: too many consecutive RTOs at seq="
                        << previous_committed << "\n";
                    return false;
                }
                reno.onTimeout();
                rto_ms = std::min(rto_ms * 2, 30000);
                if (!transmit_segment(
                        previous_committed, true)) {
                    return false;
                }
                const auto it =
                    in_flight.find(previous_committed);
                if (it != in_flight.end()) {
                    std::lock_guard<std::mutex> ack_lock(
                        aggregator.mutex);
                    aggregator.ack_window.markRetransmit(
                        previous_committed,
                        it->second.retrans_id);
                }
            }
        }
    };

    const bool transferred = transfer();
    aggregator_stop.store(true);
    aggregator.cv.notify_all();
    if (aggregator_thread.joinable()) {
        aggregator_thread.join();
    }
    if (!transferred) {
        return false;
    }

    auto wait_for_fin_acks =
        [&](uint32_t tsval, uint8_t retrans_id) {
        std::unordered_set<uint64_t> acknowledged;
        const auto deadline =
            Clock::now() +
            std::chrono::milliseconds(config_.rto_ms);
        while (Clock::now() < deadline) {
            fd_set read_fds;
            FD_ZERO(&read_fds);
            FD_SET(channel->fd, &read_fds);
            const auto remaining =
                std::chrono::duration_cast<
                    std::chrono::microseconds>(
                    deadline - Clock::now());
            timeval timeout{};
            timeout.tv_sec =
                static_cast<time_t>(
                    remaining.count() / 1000000);
            timeout.tv_usec =
                static_cast<suseconds_t>(
                    remaining.count() % 1000000);
            if (select(
                    channel->fd + 1,
                    &read_fds,
                    nullptr,
                    nullptr,
                    &timeout) <= 0) {
                break;
            }

            uint8_t frame[2048];
            const ssize_t n =
                ::recv(channel->fd, frame, sizeof(frame), 0);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK ||
                    errno == EINTR) {
                    continue;
                }
                break;
            }

            uint32_t src_ip = 0;
            uint16_t src_port = 0;
            size_t payload_len = 0;
            const uint8_t* payload = parseRenoUdpFrame(
                frame,
                n,
                mesh_result_->src_ip_n,
                channel->port,
                src_ip,
                src_port,
                payload_len);
            if (!payload || payload_len < PEEL_HEADER_SIZE) {
                continue;
            }

            PeelHeader header{};
            std::memcpy(&header, payload, sizeof(header));
            if (!peel_verify_header_checksum(header) ||
                (ntohs(header.flags) & FLG_ACK) == 0 ||
                ntohl(header.seq) != fin_seq ||
                ntohl(header.tsecr) != tsval ||
                header.retrans_id != retrans_id) {
                continue;
            }

            const uint64_t peer =
                packPeerKey(src_ip, src_port);
            if (cohort.count(peer) != 0) {
                acknowledged.insert(peer);
                if (acknowledged.size() == cohort.size()) {
                    return true;
                }
            }
        }
        return false;
    };

    constexpr int kMaxFinAttempts = 8;
    for (int attempt = 1;
         attempt <= kMaxFinAttempts;
         ++attempt) {
        const uint8_t retrans_id =
            static_cast<uint8_t>(attempt);
        const uint32_t tsval = peel_now_ms();
        if (!sendPacket(
                fin_seq,
                FLG_FIN,
                nullptr,
                0,
                retrans_id,
                tsval,
                0)) {
            return false;
        }
        if (wait_for_fin_acks(tsval, retrans_id)) {
            next_seq_ = fin_seq + 1;
            return true;
        }
    }

    std::cerr << "peel_reno[" << config_.rank
              << "]: FIN was not acknowledged\n";
    return false;
}

ssize_t PeelTransport::recvReno(
    int from_rank,
    void* data,
    size_t max_size,
    int timeout_ms) {
    if (!isReady() || (data == nullptr && max_size != 0)) {
        return -1;
    }
    if (max_size == 0) {
        return 0;
    }

    auto* channel = mesh_result_->getRecvChannel(from_rank);
    if (!channel || channel->fd < 0) {
        return -1;
    }

    auto* output = static_cast<uint8_t*>(data);
    size_t received = 0;
    bool started = false;
    uint32_t rcv_nxt = reno_recv_next_seq_;
    uint8_t last_inorder_retrans_id = 1;
    std::map<uint32_t, RenoOutOfOrder> out_of_order;

    const auto deadline =
        Clock::now() +
        std::chrono::milliseconds(
            timeout_ms > 0 ? timeout_ms : config_.timeout_ms);

    timeval socket_timeout{};
    socket_timeout.tv_usec = 100000;
    setsockopt(
        channel->fd,
        SOL_SOCKET,
        SO_RCVTIMEO,
        &socket_timeout,
        sizeof(socket_timeout));

    auto advertised_window = [&]() {
        const uint32_t capacity =
            config_.reno_ooo_buffer_segments;
        const uint32_t used =
            static_cast<uint32_t>(out_of_order.size());
        const uint32_t available =
            used >= capacity ? 1 : capacity - used;
        return static_cast<uint16_t>(
            std::min<uint32_t>(available, 0xffffu));
    };

    while (Clock::now() < deadline) {
        uint8_t frame[65536];
        const ssize_t n =
            ::recv(channel->fd, frame, sizeof(frame), 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK ||
                errno == EINTR) {
                continue;
            }
            return -1;
        }

        uint8_t sender_mac[6]{};
        if (n >= 12) {
            std::memcpy(sender_mac, frame + 6, 6);
        }

        uint32_t src_ip = 0;
        uint16_t src_port = 0;
        size_t payload_len = 0;
        const uint8_t* payload = parseRenoUdpFrame(
            frame,
            n,
            channel->mcast.sin_addr.s_addr,
            channel->port,
            src_ip,
            src_port,
            payload_len);
        if (!payload || payload_len < PEEL_HEADER_SIZE) {
            continue;
        }

        PeelHeader header{};
        std::memcpy(&header, payload, sizeof(header));
        if (!peel_verify_header_checksum(header) ||
            header.rank != static_cast<uint8_t>(from_rank)) {
            continue;
        }

        const uint16_t flags = ntohs(header.flags);
        const uint32_t seq = ntohl(header.seq);
        const uint32_t tsval = ntohl(header.tsval);
        const uint16_t sender_ack_port =
            ntohs(header.src_port);
        const uint8_t retrans_id = header.retrans_id;

        if (flags & FLG_START) {
            if (seq >= reno_recv_next_seq_) {
                started = true;
                rcv_nxt = seq;
                received = 0;
                out_of_order.clear();
                last_inorder_retrans_id = 1;
            }
            continue;
        }

        if (!started && (flags & (FLG_DATA | FLG_FIN))) {
            if (seq < reno_recv_next_seq_) {
                continue;
            }
            started = true;
            rcv_nxt = reno_recv_next_seq_;
        }

        if (flags & FLG_DATA) {
            const size_t app_len =
                payload_len - PEEL_HEADER_SIZE;

            if (seq == rcv_nxt) {
                if (received + app_len > max_size) {
                    std::cerr << "peel_reno[" << config_.rank
                              << "]: received payload exceeds buffer\n";
                    return -1;
                }
                if (app_len > 0) {
                    std::memcpy(
                        output + received,
                        payload + PEEL_HEADER_SIZE,
                        app_len);
                    received += app_len;
                }
                ++rcv_nxt;
                uint32_t ack_tsval = tsval;
                uint8_t ack_retrans_id = retrans_id;
                last_inorder_retrans_id = retrans_id;

                while (!out_of_order.empty()) {
                    auto it = out_of_order.begin();
                    if (it->first != rcv_nxt) {
                        break;
                    }
                    if (received + it->second.payload.size() >
                        max_size) {
                        std::cerr
                            << "peel_reno[" << config_.rank
                            << "]: reordered payload exceeds buffer\n";
                        return -1;
                    }
                    if (!it->second.payload.empty()) {
                        std::memcpy(
                            output + received,
                            it->second.payload.data(),
                            it->second.payload.size());
                        received += it->second.payload.size();
                    }
                    ack_tsval = it->second.tsval;
                    ack_retrans_id =
                        it->second.retrans_id;
                    last_inorder_retrans_id =
                        it->second.retrans_id;
                    out_of_order.erase(it);
                    ++rcv_nxt;
                }

                sendAck(
                    src_ip,
                    sender_ack_port,
                    sender_mac,
                    rcv_nxt,
                    ack_tsval,
                    ack_retrans_id,
                    advertised_window());
            } else if (seq > rcv_nxt) {
                if (out_of_order.size() <
                        config_.reno_ooo_buffer_segments &&
                    out_of_order.count(seq) == 0) {
                    RenoOutOfOrder entry;
                    entry.payload.assign(
                        payload + PEEL_HEADER_SIZE,
                        payload + payload_len);
                    entry.tsval = tsval;
                    entry.retrans_id = retrans_id;
                    out_of_order.emplace(
                        seq, std::move(entry));
                }

                sendAck(
                    src_ip,
                    sender_ack_port,
                    sender_mac,
                    rcv_nxt,
                    tsval,
                    last_inorder_retrans_id,
                    advertised_window());
            } else {
                // This is an already-delivered packet retransmitted because
                // its cumulative ACK was lost. Echo the incoming epoch so the
                // sender does not reject the replacement ACK as stale.
                sendAck(
                    src_ip,
                    sender_ack_port,
                    sender_mac,
                    rcv_nxt,
                    0,
                    retrans_id,
                    advertised_window());
            }
            continue;
        }

        if (flags & FLG_FIN) {
            if (seq < rcv_nxt) {
                sendAck(
                    src_ip,
                    sender_ack_port,
                    sender_mac,
                    seq,
                    tsval,
                    retrans_id,
                    1);
                continue;
            }
            if (seq != rcv_nxt || !out_of_order.empty() ||
                received != max_size) {
                std::cerr << "peel_reno[" << config_.rank
                          << "]: FIN arrived before complete data"
                          << " seq=" << seq
                          << " rcv_nxt=" << rcv_nxt
                          << " bytes=" << received << "/"
                          << max_size << "\n";
                return -1;
            }

            // FIN is a control packet sent only after every data segment is
            // cumulatively acknowledged. Replicate its ACK to avoid adding a
            // full RTO linger to every benchmark hop.
            for (int copy = 0; copy < 3; ++copy) {
                sendAck(
                    src_ip,
                    sender_ack_port,
                    sender_mac,
                    seq,
                    tsval,
                    retrans_id,
                    1);
            }
            reno_recv_next_seq_ = seq + 1;
            return static_cast<ssize_t>(received);
        }
    }

    return -1;
}

} // namespace peel
} // namespace transport
} // namespace gloo
