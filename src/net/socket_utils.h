#pragma once

#include <cstdint>

namespace kv::net {

// Creates a non-blocking TCP socket listening on all interfaces at `port`.
// SO_REUSEPORT is set so every event-loop thread can bind its own listening
// socket to the same port and the kernel spreads new connections across
// them. Pass port 0 to let the kernel pick a free port. Throws std::system_error.
int createListenSocket(uint16_t port);

// Returns the local port a socket is bound to.
uint16_t localPort(int fd);

void setNonBlocking(int fd);

// Disables Nagle's algorithm so small replies are sent immediately.
// Best effort: returns false on failure instead of throwing.
bool setNoDelay(int fd);

}  // namespace kv::net
