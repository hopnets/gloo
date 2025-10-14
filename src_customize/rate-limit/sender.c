/*********
 * 
 * Caution: 
 * 1. 32-bit may not be enough for large file
 * 2. We use dev_list[2] for fns101
 * 
 * 
 * 
 *********/


#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <infiniband/verbs.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <rdma/rdma_cma.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <netdb.h>   
#include <stdint.h>
#include <sys/select.h>
#include <fcntl.h>
#include <semaphore.h>
#include <time.h>
#include <sys/eventfd.h>
#include <stdatomic.h>

#define BATCH 100
#define CQ_LEN 2000
#define QKEY 0x12121212
#define MULTICAST_ADDR "239.255.0.1"
#define MULTICAST_MLID 0
#define MULTICAST_QPN 0xFFFFFF
#define STOP_MSG_SIZE 64
#define MAX_ACKS 32
#define ACK_MSG_SIZE 4
#define EOT_RETRIES 18
#define CONTROL_PORT "23500"
#define MAX_RECEIVERS 256

volatile int retrans_running = 1;             /* cleared when send is done   */
pthread_mutex_t qp_mutex     = PTHREAD_MUTEX_INITIALIZER;
typedef struct {
    struct Context *ctx; 
} retransmit_args_t;

typedef struct {
    char ip[64];
} receiver_entry_t;

volatile sig_atomic_t ack_received = 0;
char *send_buff;
char *file_buf;
int num_receivers;

// optional args with default value.
char port[16] = "4791";
int base_port = 4791;
int buffer_size = 960;
char payload_filename[256] = "payload.dat";
char receivers_filename[256] = "receivers.txt";
char *recv_buff = NULL;
int num_receivers = 0;
size_t payload_size = 0;
size_t file_size = 0;
int total_packets = 0; 
char *payload_buffer;
struct timespec t_start, t_end;
int received_acks = 0;
double desired_rate_mbps = 40000.0;
pthread_mutex_t ack_mutex = PTHREAD_MUTEX_INITIALIZER;

int send_buffer_len;
int recv_buffer_len;
int ack_buffer_size;
 
struct Context{
    struct ibv_device **dev_list;
    struct ibv_device *ib_dev;
    struct ibv_context *ctx;
    
    struct ibv_pd *pd;
    struct ibv_mr *mr_send;
    struct ibv_mr *mr_recv;

    struct ibv_cq *cq;
    struct ibv_qp *qp;

    struct ibv_ah *ah;
};
 
// Print help info
void print_usage(const char *progname) {
   printf("Usage: %s [options]\n", progname);
   printf("Options:\n");
   printf("  -p <port>          Starting port of RDMACM (default: 4791)\n");
   printf("  -b <buffer_size>   Chunk/buffer size in bytes (default: 1024)\n");
   printf("  -f <payload_file>  Path to payload file (default: payload.dat)\n");
   printf("  -r <receivers_file> Path to receivers list (default: receivers.txt)\n");
   printf("  -s <speed>          Set send speed in MBps\n");
   printf("  -h, -help          Show this help message and exit\n");
   printf("\n");
}
// [TODO] Comments are with help from ChatGPT and needs furhter adjusting
typedef struct {
    int                 efd;             // eventfd used as counting semaphore
    pthread_t           thread;
    pthread_mutex_t     mu;              // protects fields below
    double              tokens;          // fractional tokens (producer-side)
    double              capacity;        // burst (in tokens)
    double              rate_mbps;       // target rate in Mb/s
    double              tokens_per_sec;  // derived from rate & bytes_per_token
    uint32_t            bytes_per_token; // bytes per token (send_buffer_len)
    int                 keep_running;    // 1 = run, 0 = stop
    atomic_uint_fast64_t epoch;          // bumps on immediate rate changes
} rate_pacer_t;

static inline double ts_diff_sec(const struct timespec *a, const struct timespec *b) {
    return (double)(a->tv_sec - b->tv_sec) + (double)(a->tv_nsec - b->tv_nsec)/1e9;
}

static inline void pacer_recompute(rate_pacer_t *p) {
    const double rate_Bps = (p->rate_mbps > 0.0) ? (p->rate_mbps * 1e6 / 8.0) : 0.0;
    p->tokens_per_sec = (p->bytes_per_token > 0) ? (rate_Bps / (double)p->bytes_per_token) : 0.0;
}

// Thread-local cache so each consumer thread amortizes eventfd reads.
// If you only have one sender thread that is fine; this still helps a lot.
static inline void pacer_acquire_one(rate_pacer_t *p) {
    static __thread uint64_t local_cache = 0;
    static __thread uint64_t seen_epoch  = 0;

    uint64_t cur_epoch = atomic_load_explicit(&p->epoch, memory_order_acquire);
    if (__builtin_expect(cur_epoch != seen_epoch, 0)) {
        local_cache = 0;            // drop any stale cached tokens
        seen_epoch  = cur_epoch;
    }

    if (__builtin_expect(local_cache == 0, 0)) {
        uint64_t got = 0;
        for (;;) {
            ssize_t r = read(p->efd, &got, sizeof(got));
            if (r == sizeof(got)) break;
            if (r < 0 && errno == EINTR) continue;
            if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                // No tokens yet; block until at least 1 arrives
                // (temporarily switch to blocking read)
                int flags = fcntl(p->efd, F_GETFL, 0);
                fcntl(p->efd, F_SETFL, flags & ~O_NONBLOCK);
                r = read(p->efd, &got, sizeof(got));
                // restore nonblock
                fcntl(p->efd, F_SETFL, flags);
                if (r == sizeof(got)) break;
            }
            perror("eventfd read");
            abort();
        }
        local_cache = got;          // cache all produced tokens
    }
    --local_cache;                  // consume one token
}

static void *pacer_thread_main(void *arg) {
    rate_pacer_t *p = (rate_pacer_t *)arg;
    struct timespec prev, now;

    // Prefer RAW to avoid time adjustments, fall back if not available.
#ifdef CLOCK_MONOTONIC_RAW
    clock_gettime(CLOCK_MONOTONIC_RAW, &prev);
#else
    clock_gettime(CLOCK_MONOTONIC, &prev);
#endif

    while (__builtin_expect(p->keep_running, 1)) {
#ifdef CLOCK_MONOTONIC_RAW
        clock_gettime(CLOCK_MONOTONIC_RAW, &now);
#else
        clock_gettime(CLOCK_MONOTONIC, &now);
#endif
        double elapsed = ts_diff_sec(&now, &prev);
        prev = now;

        double tps, cap;
        uint64_t to_release = 0;

        pthread_mutex_lock(&p->mu);
        tps = p->tokens_per_sec;
        cap = p->capacity;
        p->tokens += elapsed * tps;
        if (p->tokens > cap) p->tokens = cap;

        if (p->tokens >= 1.0) {
            to_release = (uint64_t)floor(p->tokens);
            p->tokens -= (double)to_release;
        }
        pthread_mutex_unlock(&p->mu);

        if (__builtin_expect(to_release != 0, 1)) {
            // Single syscall regardless of how many tokens we produced this tick.
            if (eventfd_write(p->efd, to_release) != 0) {
                perror("eventfd_write");
                // continue; (non-fatal, but you may want to handle)
            }
        }

        // Sleep until the next token boundary (or lightly yield if tps is small).
        if (tps <= 0.0) {
            struct timespec ts = {.tv_sec = 0, .tv_nsec = 5 * 1000 * 1000}; // 5 ms idle
            nanosleep(&ts, NULL);
        } else {
            // Time until 1 more token is accumulated.
            double wait_s = (p->tokens < 1.0) ? ((1.0 - p->tokens) / tps) : (0.25 / tps);
            long ns = (long)(wait_s * 1e9);
            if (ns < 1000) ns = 1000; // floor at ~1 µs
            struct timespec ts = {.tv_sec = ns / 1000000000L, .tv_nsec = ns % 1000000000L};
            nanosleep(&ts, NULL);
        }
    }
    return NULL;
}

static int rate_pacer_start(rate_pacer_t *p, double rate_mbps, uint32_t bytes_per_token, double burst_tokens) {
    memset(p, 0, sizeof(*p));

    p->efd = eventfd(0, EFD_NONBLOCK);   // nonblocking so we can “drain” safely
    if (p->efd < 0) return -1;

    if (pthread_mutex_init(&p->mu, NULL) != 0) { close(p->efd); return -1; }

    p->tokens          = (burst_tokens > 0.0) ? burst_tokens : 1.0;
    p->capacity        = (burst_tokens >= 1.0) ? burst_tokens : 1.0;
    p->bytes_per_token = bytes_per_token;
    p->rate_mbps       = rate_mbps;
    pacer_recompute(p);

    atomic_init(&p->epoch, 1);

    // Prefill initial whole tokens into the eventfd.
    uint64_t initial = (uint64_t)floor(p->tokens);
    if (initial) {
        p->tokens -= (double)initial;
        (void)eventfd_write(p->efd, initial);
    }

    p->keep_running = 1;
    if (pthread_create(&p->thread, NULL, pacer_thread_main, p) != 0) {
        pthread_mutex_destroy(&p->mu);
        close(p->efd);
        return -1;
    }
    return 0;
}

static void rate_pacer_stop(rate_pacer_t *p) {
    p->keep_running = 0;
    (void)eventfd_write(p->efd, 1); // nudge
    pthread_join(p->thread, NULL);
    pthread_mutex_destroy(&p->mu);
    close(p->efd);
}


static void rate_pacer_update_batch_bytes(rate_pacer_t *p, uint32_t new_bytes_per_token) {
    pthread_mutex_lock(&p->mu);
    p->bytes_per_token = new_bytes_per_token;
    pacer_recompute(p);
    pthread_mutex_unlock(&p->mu);
}
static inline void pacer_gate(rate_pacer_t *p) { pacer_acquire_one(p); }
static void rate_pacer_update_rate_smooth(rate_pacer_t *p, double new_rate_mbps) {
    pthread_mutex_lock(&p->mu);
    p->rate_mbps = new_rate_mbps;
    pacer_recompute(p);
    pthread_mutex_unlock(&p->mu);
}
static void rate_pacer_update_rate_immediate(rate_pacer_t *p, double new_rate_mbps) {
    // 1) Change the rate
    pthread_mutex_lock(&p->mu);
    p->rate_mbps = new_rate_mbps;
    pacer_recompute(p);
    p->tokens = 0.0;                 // drop producer-side fractional token
    pthread_mutex_unlock(&p->mu);

    // 2) Drain any queued tokens from eventfd (nonblocking)
    for (;;) {
        uint64_t dump;
        ssize_t r = read(p->efd, &dump, sizeof(dump));
        if (r == sizeof(dump)) continue;                     // more queued
        if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break; // emptied
        if (r < 0 && errno == EINTR) continue;
        break; // other error: give up gracefully
    }

    // 3) Invalidate all sender threads’ local caches
    atomic_fetch_add_explicit(&p->epoch, 1, memory_order_release);
}

static void rate_pacer_update_rate(rate_pacer_t *p, double new_rate_mbps) {
    rate_pacer_update_rate_smooth(p, new_rate_mbps);
}


// parse arguments
void parse_args(int argc, char *argv[]) {
    // Parse optional args
    for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "-help") == 0) {
			print_usage(argv[0]);
			exit(0);
		} else if ((strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "-port") == 0) && i + 1 < argc) {
            strncpy(port, argv[i + 1], sizeof(port) - 1);
            port[sizeof(port) - 1] = '\0';
            i++;
        } else if ((strcmp(argv[i], "-b") == 0 || strcmp(argv[i], "-buffer") == 0) && i + 1 < argc) {
            buffer_size = atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-f") == 0 && i + 1 < argc) {
            strncpy(payload_filename, argv[i + 1], sizeof(payload_filename) - 1);
            payload_filename[sizeof(payload_filename) - 1] = '\0';
            i++;
        } else if (strcmp(argv[i], "-s") == 0 && i + 1 < argc) {
			char *endptr;
			double val = strtod(argv[i + 1], &endptr);
			if (*endptr != '\0' || val <= 0.0) {
				fprintf(stderr, "Invalid speed value for -s: %s\n", argv[i + 1]);
				exit(EXIT_FAILURE);
			}
			desired_rate_mbps = val;
			i++;
		} else if (strcmp(argv[i], "-r") == 0 && i + 1 < argc) {
            strncpy(receivers_filename, argv[i + 1], sizeof(receivers_filename) - 1);
            receivers_filename[sizeof(receivers_filename) - 1] = '\0';
            i++;
        } else {
            fprintf(stderr, "Unknown or malformed argument: %s\n", argv[i]);
            exit(EXIT_FAILURE);
        }
    }
	
	// Load receiver IPs from file and count them
	FILE *fp = fopen(receivers_filename, "r");
	if (!fp) {
		perror("Failed to open receivers file");
		exit(EXIT_FAILURE);
	}

	char line[256];
	num_receivers = 0;
	while (fgets(line, sizeof(line), fp)) {
		line[strcspn(line, "\r\n")] = '\0';  // remove newline
		if (line[0] != '\0') {
			num_receivers++;
		}
	}
	fclose(fp);

	if (num_receivers == 0) {
		fprintf(stderr, "Error: receivers list is empty.\n");
		exit(EXIT_FAILURE);
	}
	
    // Derive dependent sizes
    send_buffer_len = buffer_size * BATCH;
    recv_buffer_len = (buffer_size + 100) * BATCH;
    ack_buffer_size = buffer_size;
	
	recv_buff = malloc(recv_buffer_len);
	if (!recv_buff) {
		fprintf(stderr, "[Sender] Failed to allocate recv_buff (%d bytes)\n", recv_buffer_len);
		exit(EXIT_FAILURE);
	}

	// File size related
	struct stat st;
	if (stat(payload_filename, &st) != 0) {
		perror("Failed to stat payload file");
		exit(EXIT_FAILURE);
	}
	file_size = st.st_size;
	
    // Print for verification
    printf("[Sender] RDMA/RoCE Port: %s\n", port);
    printf("[Sender] Chunk Size: %d\n", buffer_size);
    printf("[Sender] Payload File: %s\n", payload_filename);
    printf("[Sender] Receivers File: %s\n", receivers_filename);
    printf("[Sender] Derived Buffer Sizes:\n");
    printf("  SEND_BUFFER_LEN = %d\n", send_buffer_len);
    printf("  RECV_BUFFER_LEN = %d\n", recv_buffer_len);
    printf("  ACK_BUFFER_SIZE = %d\n", ack_buffer_size);
	printf("[Sender] Number of receivers: %d\n", num_receivers);
	printf("[Sender] Payload file size: %zu bytes (%.6f GiB)\n",
       file_size, (double)file_size / (1024.0 * 1024.0 * 1024.0));
}

/* Listen on CONTROL_PORT for 32-bit sequence numbers to retransmit.
 * The sender re-sends the exact packet via the same multicast QP.      */
void *udp_retrans_listener(void *arg)
{
    retransmit_args_t *parg = (retransmit_args_t *)arg;
    struct Context *ctx     = parg->ctx;

    /* 1.  Bind UDP socket */
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in srv = {0};
    srv.sin_family      = AF_INET;
    srv.sin_port        = htons(atoi(CONTROL_PORT));
    srv.sin_addr.s_addr = INADDR_ANY;
    bind(s, (struct sockaddr *)&srv, sizeof(srv));

    /* 2.  Make select() time-out every second so we can exit quickly   */
    fcntl(s, F_SETFL, O_NONBLOCK);

    uint64_t wr_id_local = 0;           /* independent wr_id counter     */

    while (retrans_running) {
        fd_set rfds;  FD_ZERO(&rfds);  FD_SET(s, &rfds);
        struct timeval tv = {.tv_sec = 1, .tv_usec = 0};

        int ready = select(s + 1, &rfds, NULL, NULL, &tv);
        if (ready <= 0) continue;       /* timeout or interrupted        */

        uint32_t seq;
        ssize_t n = recvfrom(s, &seq, sizeof(seq), 0, NULL, NULL);
        if (n != sizeof(seq)) continue; /* ignore junk                   */

        if (seq >= (uint32_t)total_packets) continue;   /* out-of-range */

        size_t offset      = (size_t)seq * buffer_size;
        size_t pkt_len     = buffer_size;
        if (offset + pkt_len > payload_size)            /* last packet */
            pkt_len = payload_size - offset;

        struct ibv_sge sge = {
            .addr   = (uintptr_t)(payload_buffer + offset),
            .length = pkt_len,
            .lkey   = ctx->mr_send->lkey
        };

        struct ibv_send_wr wr = {0}, *bad = NULL;
        wr.wr_id             = ++wr_id_local;
        wr.sg_list           = &sge;
        wr.num_sge           = 1;
        wr.opcode            = IBV_WR_SEND;
        wr.send_flags        = IBV_SEND_SIGNALED;
        wr.wr.ud.ah          = ctx->ah;
        wr.wr.ud.remote_qpn  = MULTICAST_QPN;
        wr.wr.ud.remote_qkey = QKEY;

        /* 3.  Serialize access to the shared QP */
        pthread_mutex_lock(&qp_mutex);
        int res = ibv_post_send(ctx->qp, &wr, &bad);
        pthread_mutex_unlock(&qp_mutex);

        if (res) {
            perror("[Sender] ibv_post_send (retransmit)");
        }
    }

    close(s);
    free(parg);
    return NULL;
}

// Send control message to a single dest
void send_control_info(const char *receiver_ip, int port_number) {
    int sockfd;
    struct addrinfo hints, *res;
    char message[128];
	char port_str[6];

	snprintf(port_str, sizeof(port_str), "%d", port_number);
    snprintf(message, sizeof(message), "%d %zu %s", buffer_size, payload_size, port_str);
    printf("[Sender] Sending control message: %s\n", message);

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(receiver_ip, CONTROL_PORT, &hints, &res) != 0) {
        perror("[Sender] getaddrinfo");
        exit(1);
    }

    sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sockfd < 0) {
        perror("[Sender] socket");
        exit(1);
    }

    if (connect(sockfd, res->ai_addr, res->ai_addrlen) < 0) {
        perror("[Sender] connect");
        close(sockfd);
        exit(1);
    }

    send(sockfd, message, strlen(message), 0);
    printf("[Sender] Control message sent.\n");

    close(sockfd);
    freeaddrinfo(res);
}

// Send control signal to all receivers
void send_control_to_all_receivers() {
    FILE *fp = fopen(receivers_filename, "r");
    if (!fp) {
        perror("Failed to open receivers file");
        exit(EXIT_FAILURE);
    }

    receiver_entry_t receivers[MAX_RECEIVERS];
    size_t count = 0;

    while (count < MAX_RECEIVERS && fgets(receivers[count].ip, sizeof(receivers[count].ip), fp)) {
        receivers[count].ip[strcspn(receivers[count].ip, "\r\n")] = '\0';  // strip newline
        if (receivers[count].ip[0] != '\0') {
            ++count;
        }
    }

    fclose(fp);

    if (count == 0) {
        fprintf(stderr, "No valid receivers found.\n");
        exit(EXIT_FAILURE);
    }
    printf("[Sender] Sending control messages to %zu receivers...\n", count);
    for (size_t i = 0; i < count; ++i) {
        int dynamic_port = base_port + (int)(i * 2);
        send_control_info(receivers[i].ip, dynamic_port);
    }
}

int init(struct Context *context){
    int ret;

    /* 0. Init device */
    int num_device = 0;
    context->dev_list = ibv_get_device_list(&num_device);
    if(!context->dev_list || !num_device){
        fprintf(stderr, "No IB devices found\n");
        return 1;
    }
    for(int i = 0; i < num_device; i++){
        printf("%s\n",context->dev_list[i]->name);
        if(strcmp(context->dev_list[i]->name, "mlx5_1") == 0){
	    context->ib_dev = context->dev_list[i];
	    printf("Device number: %d\n",i);
	}
    }
    //context->ib_dev = context->dev_list[2];
    context->ctx = ibv_open_device(context->ib_dev);
    if (!context->ctx) {
        fprintf(stderr, "Failed to open device\n");
        return 1;
    }

    /* 1. Allocate PD */
    context->pd = ibv_alloc_pd(context->ctx);
    if (!context->pd) {
        fprintf(stderr, "Failed to allocate PD\n");
        return 1;
    }

    /* 2. Register MR*/
    context->mr_recv = ibv_reg_mr(context->pd, recv_buff, recv_buffer_len, IBV_ACCESS_LOCAL_WRITE);
    if(!context->mr_recv){
        fprintf(stderr, "Failed to register memory, errno: %d\n", errno);
        return 1;
    }

    /* 3. Create CQ*/
    context->cq = ibv_create_cq(context->ctx, CQ_LEN, NULL, NULL, 0);
    if(!context->cq){
        fprintf(stderr, "Failed to create CQ\n");
        return 1;
    }
 
    /* 5. Create UP QP*/
    {
        struct ibv_qp_init_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.qp_type          = IBV_QPT_UD;
        attr.send_cq          = context->cq;
        attr.recv_cq          = context->cq;
        attr.cap.max_send_wr  = CQ_LEN;
        attr.cap.max_recv_wr  = CQ_LEN;
        attr.cap.max_send_sge = 1;
        attr.cap.max_recv_sge = 1;
        context->qp = ibv_create_qp(context->pd, &attr);
        if(!context->qp){
            fprintf(stderr, "Failed to create QP\n");
            return 1;
        }
    }
 
    /* 6. Transit QP states*/
    {
        struct ibv_qp_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_INIT;
        attr.pkey_index = 0;
        attr.port_num   = 1;
        attr.qkey       = QKEY;
        
        ret = ibv_modify_qp(context->qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                                                IBV_QP_PORT  | IBV_QP_QKEY);
        if(ret){
            fprintf(stderr, "Failed to modify QP to INIT, ret = %d\n", ret);
            return 1;
        }
    
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTR;
        ret = ibv_modify_qp(context->qp, &attr, IBV_QP_STATE);
        if (ret) {
            fprintf(stderr, "Failed to modify QP to RTR, ret = %d\n", ret);
            return 1;
        }

        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTS;
        attr.sq_psn   = 0;
        ret = ibv_modify_qp(context->qp, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN);
        if (ret) {
            fprintf(stderr, "Failed to modify QP to RTS, ret = %d\n", ret);
            return 1;
        }
    }


    /* 7. Join multicast group*/
    {
        union ibv_gid mgid;
        memset(&mgid, 0, sizeof(mgid));
        mgid.raw[10] = 0xff;
        mgid.raw[11] = 0xff;

        if (inet_pton(AF_INET, MULTICAST_ADDR, &mgid.raw[12]) != 1) {
            perror("inet_pton");
            return 1;
        }
        
        if(ibv_attach_mcast(context->qp, &mgid, MULTICAST_MLID)){
            perror("Failed to attach to multicast group");
            return 1;
        }
        
        printf("[Sender] Joined multicast group: %s\n", MULTICAST_ADDR);  
    }

    /* 8. Create AH*/
    {
        int gid_index = -1;
        for (int i = 0; i < 16; i++) {
            struct ibv_gid_entry entry;
            if (ibv_query_gid_ex(context->ctx, 1, i, &entry, 0) == 0) {
                printf("[Sender] GID index %d: ", i);
                for (int j = 0; j < 16; j++) {
                    printf("%02x", entry.gid.raw[j]);
                    if(j < 15) printf(":");
                }
                printf("\n");
		printf("type = %u\n", entry.gid_type);
                // If non-zero GID found, use it
		//printf("%d %d %d\n",entry.gid.raw[10] == 0xff, entry.gid.raw[11] == 0xff, entry.gid_type == 2);
                if (entry.gid.raw[10] == 0xff && entry.gid.raw[11] == 0xff && entry.gid_type == 2) {
                    gid_index = i;
		    printf("[Sender] Using GID index %d\n", gid_index);
                    
                    break;
                }
            }
        }

        if (gid_index == -1) {
            fprintf(stderr, "Failed to find valid GID index\n");
            return 1;
        }

        union ibv_gid mgid;
        memset(&mgid, 0, sizeof(mgid));
        mgid.raw[10] = 0xff;
        mgid.raw[11] = 0xff;
        if (inet_pton(AF_INET, MULTICAST_ADDR, &mgid.raw[12]) != 1) {
            perror("inet_pton");
            return 1;
        }

        struct ibv_ah_attr ah_attr;
        memset(&ah_attr, 0, sizeof(ah_attr));
        ah_attr.is_global = 1;
        ah_attr.dlid = 0;
        ah_attr.sl = 0;
        ah_attr.port_num = 1;
        ah_attr.grh.hop_limit = 64;
        ah_attr.grh.sgid_index = gid_index;
        ah_attr.grh.dgid = mgid;

        context->ah = ibv_create_ah(context->pd, &ah_attr);
        if (!context->ah) {
            fprintf(stderr, "Failed to create Address Handle for multicast\n");
            return 1;
        }
        printf("[Sender] Sending to multicast GID: ");
        for (int i = 0; i < 16; i++) {
            printf("%02x", mgid.raw[i]);
            if (i % 2 == 1) printf(":");
        }
        printf("\n");
    }

}
 
void clean(struct Context *context){

    ibv_free_device_list(context->dev_list);
    ibv_destroy_ah(context->ah);
    ibv_dereg_mr(context->mr_send);
    ibv_dereg_mr(context->mr_recv);
    ibv_destroy_qp(context->qp);
    ibv_destroy_cq(context->cq);
    ibv_dealloc_pd(context->pd);
    ibv_close_device(context->ctx);
}

void *ack_listener_thread(void *arg) {
    int port = *((int *)arg);
    free(arg);

    printf("[ACK Listener] Waiting for ACK on port %d\n", port);

    struct rdma_event_channel *ec = rdma_create_event_channel();
    struct rdma_cm_id *listener = NULL, *id = NULL;
    struct rdma_cm_event *event = NULL;
    struct sockaddr_in addr;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP);
    rdma_bind_addr(listener, (struct sockaddr *)&addr);
    rdma_listen(listener, 1); // one ACK per port

    if (rdma_get_cm_event(ec, &event)) {
        perror("rdma_get_cm_event");
        goto cleanup;
    }

    id = event->id;
    rdma_ack_cm_event(event);

    struct ibv_pd *pd = ibv_alloc_pd(id->verbs);
    char *recv_buf = malloc(buffer_size);
    struct ibv_mr *mr = ibv_reg_mr(pd, recv_buf, buffer_size, IBV_ACCESS_LOCAL_WRITE);
    struct ibv_comp_channel *comp_chan = ibv_create_comp_channel(id->verbs);
    struct ibv_cq *cq = ibv_create_cq(id->verbs, 1, NULL, comp_chan, 0);
    ibv_req_notify_cq(cq, 0);

    struct ibv_qp_init_attr qp_attr = {
        .send_cq = cq, .recv_cq = cq,
        .cap = {.max_send_wr = 1, .max_recv_wr = 1, .max_send_sge = 1, .max_recv_sge = 1},
        .qp_type = IBV_QPT_RC
    };

    rdma_create_qp(id, pd, &qp_attr);

    struct rdma_conn_param conn_param = {.responder_resources = 1, .initiator_depth = 1};
    rdma_accept(id, &conn_param);

    // Post recv
    struct ibv_sge sge = {.addr = (uintptr_t)recv_buf, .length = buffer_size, .lkey = mr->lkey};
    struct ibv_recv_wr wr = {.wr_id = 0, .next = NULL, .sg_list = &sge, .num_sge = 1};
    struct ibv_recv_wr *bad_wr;
    ibv_post_recv(id->qp, &wr, &bad_wr);

    // Wait for CQ
    struct ibv_cq *ev_cq;
    void *ev_ctx;
    if (ibv_get_cq_event(comp_chan, &ev_cq, &ev_ctx) == 0) {
        ibv_ack_cq_events(cq, 1);
        ibv_req_notify_cq(cq, 0);

        struct ibv_wc wc;
        if (ibv_poll_cq(cq, 1, &wc) == 1 && wc.status == IBV_WC_SUCCESS) {
			clock_gettime(CLOCK_MONOTONIC, &t_end);
			pthread_mutex_lock(&ack_mutex);
			received_acks++;
			if (received_acks == num_receivers) {
				ack_received = 1;
			}
			pthread_mutex_unlock(&ack_mutex);

            printf("[ACK Listener] Received ACK on port %d: %.16s\n", port, recv_buf);
			/*
            pthread_mutex_lock(&ack_mutex);
            received_acks++;
            if (received_acks == num_receivers) {
                ack_received = 1;
            }
            pthread_mutex_unlock(&ack_mutex);
			*/
        } else {
            fprintf(stderr, "[ACK Listener] Failed to poll CQ on port %d\n", port);
        }
    }

cleanup:
    if (id) {
        rdma_disconnect(id);
        rdma_destroy_qp(id);
        rdma_destroy_id(id);
    }
    if (listener) rdma_destroy_id(listener);
    if (ec) rdma_destroy_event_channel(ec);
    // Free resources
    if (mr) ibv_dereg_mr(mr);
    if (pd) ibv_dealloc_pd(pd);
    if (cq) ibv_destroy_cq(cq);
    if (comp_chan) ibv_destroy_comp_channel(comp_chan);
    if (recv_buf) free(recv_buf);
    return NULL;
}

void launch_ack_threads() {
    pthread_t tids[num_receivers];

    for (int i = 0; i < num_receivers; i++) {
        int *port_ptr = malloc(sizeof(int));
        *port_ptr = base_port + i * 2;  // Dynamically assign port
        pthread_create(&tids[i], NULL, ack_listener_thread, port_ptr);
    }

    // Optionally join threads or poll ack_received
}

static int send_data_with_pacer(struct Context *context,
                                uint8_t *payload_buffer,
                                size_t payload_size,
                                size_t buffer_size,
                                size_t send_buffer_len,
                                rate_pacer_t *pacer)   // << pacer handle
{
    size_t position = 0;
    uint64_t id = 0;
    int total_pkt_send = 0;

    while (position < payload_size) {
        size_t bytes_to_read = send_buffer_len;
        if (position + send_buffer_len > payload_size) {
            bytes_to_read = payload_size - position;
        }

        struct ibv_sge     sge[BATCH];
        struct ibv_send_wr wr[BATCH], *bad_wr = NULL;
        memset(sge, 0, sizeof(sge));
        memset(wr,  0, sizeof(wr));

        size_t current = bytes_to_read;
        int batch_size = (int)ceil((double)current / (double)buffer_size);
        if (batch_size > BATCH) {
            fprintf(stderr, "[Sender] batch_size (%d) > BATCH (%d)\n", batch_size, BATCH);
            return 1;
        }

        for (int i = 0; i < batch_size; i++) {
            size_t offset    = position + (size_t)i * buffer_size;
            size_t remaining = payload_size - offset;
            if (remaining == 0) break;

            size_t chunk_size = (remaining >= buffer_size) ? buffer_size : remaining;

            sge[i].addr   = (uintptr_t)(payload_buffer + offset);
            sge[i].length = chunk_size;
            sge[i].lkey   = context->mr_send->lkey;

            wr[i].wr_id      = ++id;
            wr[i].sg_list    = &sge[i];
            wr[i].num_sge    = 1;
            wr[i].opcode     = IBV_WR_SEND;
            wr[i].wr.ud.ah   = context->ah;
            wr[i].wr.ud.remote_qpn  = MULTICAST_QPN;
            wr[i].wr.ud.remote_qkey = QKEY;

            if (i == batch_size - 1 || chunk_size < buffer_size || offset + buffer_size >= payload_size) {
                int tail = (int)(current % buffer_size);
                sge[i].length = tail ? (size_t)tail : buffer_size;
                wr[i].send_flags = IBV_SEND_SIGNALED;
                wr[i].next = NULL;
            } else {
                wr[i].next = &wr[i + 1];
            }
            ++total_pkt_send;
        }

        // === PACER GATE: one token per *batch* ===
        //while (sem_wait(&pacer->sem) == -1 && errno == EINTR) { /* retry on signal */ }
		pacer_gate(pacer);
        if (ibv_post_send(context->qp, &wr[0], &bad_wr)) {
            perror("Failed to send multicast message");
            return 1;
        }

        struct ibv_wc wc;
        while (ibv_poll_cq(context->cq, 1, &wc) < 1) { /* Waiting... */ }
        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "Send failed with status: %d\n", wc.status);
        }

        position += bytes_to_read;
    }
    return 0;
}

int send_multicast(struct Context *context, int argc, char *argv[]){
    
	// Get file handle 
	FILE *fp = fopen(payload_filename, "rb");
	if (!fp) {
		perror("Failed to open payload file");
		exit(EXIT_FAILURE);
	}
	struct stat st;
	char *file_buf = NULL;
	//size_t file_size = 0;
	//payload_size = file_size;
    size_t total = 0, current = 0;
    size_t id = 0;
	double elapsed, file_size_gib, buffer_len_gib, bandwidth_gbps;
	
	
	file_size_gib = (double)file_size / (1024.0*1024.0*1024.0);
	total_packets = (file_size + (buffer_size - 5)) / (buffer_size - 4); 
	printf("[Sender] Total Packets: %d\n", total_packets);
	

	payload_size = total_packets * buffer_size;
	payload_buffer = malloc(payload_size);
	if (!payload_buffer) {
		perror("malloc failed");
		fclose(fp);
		return 1;
	}
	//read payload file in to memory
	char *file_buffer = malloc(file_size);
	if (!file_buffer) {
		perror("malloc failed for file_buffer");
		free(payload_buffer);
		fclose(fp);
		return 1;
	}

	size_t read_size = fread(file_buffer, 1, file_size, fp);
	if (read_size != file_size) {
		fprintf(stderr, "[Sender] Warning: Read %zu bytes, expected %zu\n", read_size, file_size);
	}
	fclose(fp);
	
	// Fill the packetized payload_buffer
	for (int i = 0; i < total_packets; ++i) {
		char *packet_ptr = payload_buffer + i * buffer_size;

		// Insert packet sequence number
		uint32_t seq_num = i;
		memcpy(packet_ptr, &seq_num, sizeof(uint32_t));

		// Copy corresponding payload segment
		size_t data_offset = i * (buffer_size - 4);
		size_t remaining = read_size > data_offset ? read_size - data_offset : 0;
		size_t chunk_size = remaining > (buffer_size - 4) ? (buffer_size - 4) : remaining;

		memcpy(packet_ptr + 4, file_buffer + data_offset, chunk_size);
	}

	free(file_buffer);
	// Stage 3.5: start listening for ACK before actual payload
	launch_ack_threads();	

	send_control_to_all_receivers();
	
	// Wait 3 seconds to make sure all recievers have recv posted
	sleep(3);

	// Reg MR

	context->mr_send = ibv_reg_mr(context->pd, payload_buffer, payload_size, IBV_ACCESS_LOCAL_WRITE);
    if(!context->mr_send){
        fprintf(stderr, "Failed to register memory, errno: %d\n", errno);
        return 1;
    }
	
	//Start retrans thread
	pthread_t retrans_thread;
	retransmit_args_t *rt_args = malloc(sizeof(*rt_args));
	rt_args->ctx = context;
	pthread_create(&retrans_thread, NULL, udp_retrans_listener, rt_args);
    printf("=== Multicast send test START ===\n");

    // report key parameters
    buffer_len_gib = (double)send_buffer_len / (1024.0);
	


	// Stage 4: send and time the transfer
    clock_gettime(CLOCK_MONOTONIC, &t_start);
	size_t position = 0;
	uint32_t seq_num = 0;
	printf("Multicasting...\n");
	int total_pkt_send = 0;
	
	/* How to change rate at live 
	// Increase gently to 20 Gbps (preserves current burst/backlog)
	rate_pacer_update_rate_smooth(&pacer, 20000.0);

	// Abruptly throttle to 1 Gbps *now* (no leftover fast burst)
	rate_pacer_update_rate_immediate(&pacer, 1000.0);
	*/
	
	
	
	rate_pacer_t pacer;
	
	if (rate_pacer_start(&pacer,
						 desired_rate_mbps,
						 (uint32_t)send_buffer_len,   // 1 token == one *batch* of this many bytes
						 /*burst_tokens=*/2.0) != 0)  // small burst for jitter tolerance
	{
		fprintf(stderr, "Failed to start pacer\n");
		return 1;
	}
	// sample speed change
	// rate_pacer_update_rate_smooth(&pacer, 20000.0);

	// Do the paced send:
	int rc = send_data_with_pacer(context,
								  (uint8_t*)payload_buffer,
								  payload_size,
								  buffer_size,
								  send_buffer_len,
								  &pacer);

	// Clean up our own new resources (no changes to your existing cleanup)
	rate_pacer_stop(&pacer);
	if (rc != 0) return rc;


	
	/*
    while (position < payload_size) {
		size_t bytes_to_read = send_buffer_len;
		if (position + send_buffer_len > payload_size) {
			bytes_to_read = payload_size - position;
		}
		
        struct ibv_sge      sge[BATCH];
        struct ibv_send_wr  wr[BATCH], *bad_wr;
        memset(sge, 0, sizeof(sge));
        memset(wr,  0, sizeof(wr));
 
        //printf("%ldB read\n",current);
		current = (payload_size - position) > send_buffer_len ? send_buffer_len : (payload_size - position);
        int batch_size = ceil(current * 1.0 / buffer_size);
		
        for(int i = 0; i < batch_size; i++){
			size_t offset = position + i * buffer_size;
			size_t remaining = payload_size - offset;
			if (remaining == 0) break;
			size_t chunk_size = (remaining >= buffer_size) ? buffer_size : remaining;
            //sge[i].addr = (uintptr_t)(send_buff + i * CHUNK);
			sge[i].addr = (uintptr_t)(payload_buffer + offset);
			
			// print content
			//uint8_t *chunk_data = (uint8_t *)(payload_buffer + offset);
			//printf("[Packet %d] First 8 bytes: ", total_pkt_send);
			//for (int b = 0; b < 8 && b < chunk_size; ++b) {
			//	printf("%02X ", chunk_data[b]);
			//}
			//printf("\n");
			
			
            sge[i].length = chunk_size;
            sge[i].lkey = context->mr_send->lkey;
 
            wr[i].wr_id = ++id;
            wr[i].sg_list = &sge[i];
            wr[i].num_sge = 1;
            wr[i].opcode = IBV_WR_SEND;
            wr[i].wr.ud.ah = context->ah;
            wr[i].wr.ud.remote_qpn = MULTICAST_QPN;
            wr[i].wr.ud.remote_qkey = QKEY;
            if (i == batch_size - 1 || chunk_size < buffer_size || offset + buffer_size >= payload_size) {
               int tail = current % buffer_size;
               sge[i].length = tail ? tail : buffer_size;
               wr[i].send_flags = IBV_SEND_SIGNALED;
               wr[i].next = NULL;
            }else{
               wr[i].next = &wr[i + 1];
            }
			++total_pkt_send;
			//printf("Total Sent Packets: %d\n", total_pkt_send);
			//usleep(350); //sleep 0.35 ms
        }
 
         
        if (ibv_post_send(context->qp, &wr[0], &bad_wr)) {
            perror("Failed to send multicast message");
            return 1;
        }
 
        struct ibv_wc wc;
        while (ibv_poll_cq(context->cq, 1, &wc) < 1) {
            // Waiting...
        }
         
        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "Send failed with status: %d\n", wc.status);
        } 
 
        total += current;
		position += bytes_to_read;

        //printf("\n %dB has been sent.\n %d ids have been used.\n", total, id);
 
    }
	
	*/
	// Send the stop signal
	uint8_t *stop_buf;
	posix_memalign((void**)&stop_buf, sysconf(_SC_PAGESIZE), STOP_MSG_SIZE);
	memset(stop_buf, 0, STOP_MSG_SIZE);
	*(uint32_t*)stop_buf = 0xFFFFFFFF;

	struct ibv_mr* mr_stop = ibv_reg_mr(context->pd, stop_buf, STOP_MSG_SIZE, IBV_ACCESS_LOCAL_WRITE);
	if (!mr_stop) {
		fprintf(stderr, "Failed to register STOP buffer\n");
		return 1;
	}

	struct ibv_sge sge_stop = {
		.addr   = (uintptr_t)stop_buf,
		.length = STOP_MSG_SIZE,
		.lkey   = mr_stop->lkey
	};
	
	
	
	for (int retry = 0; retry < EOT_RETRIES; retry++) {
		if (ack_received) {
			printf("ACK already received — skipping remaining EOT packets.\n");
			break;
		}
		struct ibv_send_wr wr_stop = {
			.wr_id             = ++id,
			.sg_list           = &sge_stop,
			.num_sge           = 1,
			.opcode            = IBV_WR_SEND,
			.send_flags        = IBV_SEND_SIGNALED,
			.wr.ud.ah          = context->ah,
			.wr.ud.remote_qpn  = MULTICAST_QPN,
			.wr.ud.remote_qkey = QKEY,
			.next              = NULL
		};

		struct ibv_send_wr *bad_wr;
		if (ibv_post_send(context->qp, &wr_stop, &bad_wr)) {
			perror("Failed to send STOP message");
			return 1;
		}

		struct ibv_wc wc_stop;
		while (ibv_poll_cq(context->cq, 1, &wc_stop) < 1);
		if (wc_stop.status != IBV_WC_SUCCESS) {
			fprintf(stderr, "STOP send failed with status: %d\n", wc_stop.status);
		} else {
			printf("STOP packet #%d sent (seq_id = %ld, first 4 bytes: 0x%08X)\n",
				   retry + 1, wr_stop.wr_id, *(uint32_t*)stop_buf);
		}

		usleep(150000);  // 50 ms between stop messages
	}
	
	// Wait for ACK if not yet received
	int wait_ms = 5000;  // wait up to 5 seconds in 50ms intervals
	int waited = 0;
	while (!ack_received && waited < wait_ms) {
		usleep(50000);  // 50 ms
		waited += 50;
	}

	/* ----- stop UDP retransmission listener ----- */
	retrans_running = 0;
	/* poke the socket so select() wakes up immediately */
	{
		int poke = socket(AF_INET, SOCK_DGRAM, 0);
		struct sockaddr_in dst = {.sin_family = AF_INET,
								  .sin_port   = htons(atoi(CONTROL_PORT)),
								  .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
		sendto(poke, "", 0, 0, (struct sockaddr *)&dst, sizeof(dst));
		close(poke);
	}
	pthread_join(retrans_thread, NULL);


    printf("\n\n=== Multicast Send Test Report ===\n");

	printf("  PARAMETERS:\n");
    printf("    CHUNK         	= %d bytes\n", buffer_size);
    printf("    BATCH         	= %d\n", BATCH);
	printf("    Message Size   	= %ld Byte(s)\n", payload_size);
    printf("    MULTICAST_ADDR	= %s\n", MULTICAST_ADDR);
    printf("    SEND_BUFFER_LEN	= %.6f KiB\n", buffer_len_gib);
    printf("    CQ_LEN        	= %d\n", CQ_LEN);
    printf("    QKEY          	= 0x%x\n", QKEY);
    printf("    MULTICAST_QPN 	= 0x%x\n", MULTICAST_QPN);
	if (desired_rate_mbps > 0.0) {
		printf("    Speed Limit   	= %.3f Mbps (%.3f Gbps)\n", desired_rate_mbps, desired_rate_mbps / 1000.0);
	}

	// compute stats
    elapsed       = (t_end.tv_sec - t_start.tv_sec)
                    + (t_end.tv_nsec - t_start.tv_nsec)/1e9;
    bandwidth_gbps= (file_size * 8.0) / (elapsed * 1e9);
    double aggregated_bandwidth = bandwidth_gbps * num_receivers;
	double bandwidth_mbps = bandwidth_gbps * 1000.0;
    double aggregated_bandwidth_mbps = aggregated_bandwidth * 1000.0;
    printf("\nBenchmark Result:\n");
    printf("  Data sent           : %.6f GiB\n", file_size_gib);
	printf("============= Bandwidth in Gbps =============\n");
	printf("  Target Bandwidth    : %.6f Gbps\n", desired_rate_mbps / 1000.0);
    printf("  Bandwidth           : %.6f Gbps\n", bandwidth_gbps);
    printf("  Aggregated Bandwidth: %.6f Gbps (across %d receivers)\n", aggregated_bandwidth, num_receivers);
	printf("============= Bandwidth in Mbps =============\n");
	printf("  Target Bandwidth    : %.6f Mbps\n", desired_rate_mbps);
	printf("  Bandwidth           : %.6f Mbps\n", bandwidth_mbps);
    printf("  Aggregated Bandwidth: %.6f Mbps (across %d receivers)\n", aggregated_bandwidth_mbps, num_receivers);
	printf("========= Flow Completion Time (FCT) =========\n");
	printf("  Flow Completion Time: %.6f seconds\n", elapsed);
	printf("  Flow Completion Time: %.3f ms\n", elapsed * 1000.0);
	
    clean(context);
}
  
int main(int argc, char *argv[]) { 
    struct Context context;
    memset(&context, 0, sizeof(context));
	parse_args(argc, argv);
	
    init(&context);
    send_multicast(&context, argc, argv);
    //receive_multicast(&context);
    return 0;
}
 
