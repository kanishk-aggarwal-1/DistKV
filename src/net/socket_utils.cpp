#include "net/socket_utils.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <system_error>

namespace kv::net {

namespace {

[[noreturn]] void throwErrno(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}

}  // namespace

int createListenSocket(uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) throwErrno("socket");

  int one = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0 ||
      ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0) {
    int saved = errno;
    ::close(fd);
    errno = saved;
    throwErrno("setsockopt");
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0 ||
      ::listen(fd, SOMAXCONN) < 0) {
    int saved = errno;
    ::close(fd);
    errno = saved;
    throwErrno("bind/listen");
  }
  return fd;
}

uint16_t localPort(int fd) {
  sockaddr_in addr{};
  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) throwErrno("getsockname");
  return ntohs(addr.sin_port);
}

void setNonBlocking(int fd) {
  int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) throwErrno("fcntl");
}

bool setNoDelay(int fd) {
  int one = 1;
  return ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) == 0;
}

}  // namespace kv::net
