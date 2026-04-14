set(GLOO_BENCHMARK_SRCS
  "${CMAKE_CURRENT_SOURCE_DIR}/main.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/options.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/runner.cc"
  )

add_executable(benchmark ${GLOO_BENCHMARK_SRCS})
target_link_libraries(benchmark gloo)

# Peel benchmark needs hiredis (PeelDiscovery/PeelRedis use it directly;
# symbols are not bundled in libgloo.a).
find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
  pkg_check_modules(HIREDIS hiredis)
  if(HIREDIS_FOUND)
    target_link_libraries(benchmark ${HIREDIS_LIBRARIES})
    target_include_directories(benchmark PRIVATE ${HIREDIS_INCLUDE_DIRS})
  endif()
endif()

if(GLOO_INSTALL)
  install(TARGETS benchmark DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
endif()

if(USE_CUDA)
  set(GLOO_BENCHMARK_CUDA_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/cuda_main.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/options.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/runner.cc"
    )

  add_executable(benchmark_cuda ${GLOO_BENCHMARK_CUDA_SRCS})
  target_link_libraries(benchmark_cuda gloo_cuda ${GLOO_CUDA_LIBRARIES})

  if(GLOO_INSTALL)
    install(TARGETS benchmark_cuda DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
  endif()
endif()

if(USE_ROCM)
  set(GLOO_BENCHMARK_HIP_SRCS
    "${HIPIFY_OUTPUT_DIR}/benchmark/hip_main.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/options.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/runner.cc"
    )

  gloo_hip_add_executable(benchmark_hip ${GLOO_BENCHMARK_HIP_SRCS})
  target_link_libraries(benchmark_hip gloo_hip)

  if(GLOO_INSTALL)
    install(TARGETS benchmark_hip DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
  endif()
endif()
