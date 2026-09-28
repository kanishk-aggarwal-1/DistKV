cmake_minimum_required(VERSION 3.22)
project(distkv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(KV_BUILD_TESTS "Build the test suite" ON)
set(KV_SANITIZE "" CACHE STRING "Sanitizer to build with: empty, 'address' (ASan+UBSan) or 'thread' (TSan)")

# Sanitizer flags are applied globally so that GoogleTest is instrumented too;
# mixing instrumented and uninstrumented code gives TSan false positives.
if(KV_SANITIZE STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-sanitize-recover=undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
elseif(KV_SANITIZE STREQUAL "thread")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
elseif(NOT KV_SANITIZE STREQUAL "")
  message(FATAL_ERROR "Unknown KV_SANITIZE value: ${KV_SANITIZE}")
endif()

find_package(Threads REQUIRED)

add_library(kvcore STATIC
  src/protocol/resp_parser.cpp
  src/protocol/resp_writer.cpp
  src/storage/store.cpp
  src/net/socket_utils.cpp
  src/net/connection.cpp
  src/net/event_loop.cpp
  src/server/command_handler.cpp
  src/server/server.cpp
)
target_include_directories(kvcore PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_compile_options(kvcore PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(kvcore PUBLIC Threads::Threads)

add_executable(distkv-server src/server/main.cpp)
target_compile_options(distkv-server PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(distkv-server PRIVATE kvcore)

if(KV_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
