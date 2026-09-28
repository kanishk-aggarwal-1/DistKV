#pragma once

// The tests use the same blocking RESP clients as distkv-verifier.
#include "client/resp_client.h"

namespace kv::testing {
using namespace kv::client;
}  // namespace kv::testing
