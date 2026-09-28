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
# (gRPC comes from system packages and is not instrumented; see DESIGN.md.)
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
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

# ---- Generated gRPC/protobuf code -------------------------------------------
set(PROTO_FILE ${PROJECT_SOURCE_DIR}/proto/distkv.proto)
set(PROTO_OUT ${CMAKE_CURRENT_BINARY_DIR}/generated)
set(PROTO_SRCS
  ${PROTO_OUT}/distkv.pb.cc ${PROTO_OUT}/distkv.pb.h
  ${PROTO_OUT}/distkv.grpc.pb.cc ${PROTO_OUT}/distkv.grpc.pb.h)
file(MAKE_DIRECTORY ${PROTO_OUT})
add_custom_command(
  OUTPUT ${PROTO_SRCS}
  COMMAND protobuf::protoc
          --proto_path=${PROJECT_SOURCE_DIR}/proto
          --cpp_out=${PROTO_OUT}
          --grpc_out=${PROTO_OUT}
          --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
          ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating gRPC code from distkv.proto")

add_library(distkv_proto STATIC ${PROTO_SRCS})
# SYSTEM: warnings in generated code are not ours to fix.
target_include_directories(distkv_proto SYSTEM PUBLIC ${PROTO_OUT})
target_link_libraries(distkv_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

# ---- Core library -----------------------------------------------------------
add_library(distkv_core STATIC
  src/protocol/resp_parser.cpp
  src/protocol/resp_writer.cpp
  src/storage/store.cpp
  src/net/socket_utils.cpp
  src/net/connection.cpp
  src/net/event_loop.cpp
  src/cluster/slot.cpp
  src/cluster/hash_ring.cpp
  src/cluster/cluster_map.cpp
  src/cluster/cluster_state.cpp
  src/cluster/rpc.cpp
  src/cluster/node_service.cpp
  src/coordinator/coordinator.cpp
  src/server/command_handler.cpp
  src/server/server.cpp
)
target_include_directories(distkv_core PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_compile_options(distkv_core PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(distkv_core PUBLIC distkv_proto Threads::Threads)

# ---- Executables ------------------------------------------------------------
foreach(target_and_source
    "distkv-server:src/server/main.cpp"
    "distkv-coordinator:src/coordinator/main.cpp"
    "distkv-admin:src/tools/admin_main.cpp")
  string(REPLACE ":" ";" parts ${target_and_source})
  list(GET parts 0 name)
  list(GET parts 1 source)
  add_executable(${name} ${source})
  target_compile_options(${name} PRIVATE -Wall -Wextra -Wpedantic)
  target_link_libraries(${name} PRIVATE distkv_core)
endforeach()

if(KV_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
