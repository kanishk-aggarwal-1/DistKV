include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)

add_executable(unit_tests
  unit/resp_parser_test.cpp
  unit/command_handler_test.cpp
  unit/store_test.cpp
  unit/slot_test.cpp
  unit/hash_ring_test.cpp
  unit/replication_test.cpp
)
target_link_libraries(unit_tests PRIVATE distkv_core GTest::gtest_main)
# PRE_TEST: list tests when ctest runs, not at build time, so that the TSan
# binaries only execute under the ASLR workaround in scripts/check.sh.
gtest_discover_tests(unit_tests DISCOVERY_MODE PRE_TEST)

# In-process servers exercised over real TCP sockets.
add_executable(server_tests
  integration/server_test.cpp
  integration/cluster_test.cpp
)
target_include_directories(server_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(server_tests PRIVATE distkv_core GTest::gtest_main)
gtest_discover_tests(server_tests DISCOVERY_MODE PRE_TEST)

# End-to-end checks with the real redis-cli, when it is installed.
find_program(REDIS_CLI redis-cli)
if(REDIS_CLI)
  add_test(NAME redis_cli_integration
           COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/integration/redis_cli_test.sh
                   $<TARGET_FILE:distkv-server>)
  add_test(NAME redis_cli_cluster_integration
           COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/integration/redis_cli_cluster_test.sh
                   $<TARGET_FILE_DIR:distkv-server>)
else()
  message(STATUS "redis-cli not found; skipping redis-cli integration tests")
endif()
