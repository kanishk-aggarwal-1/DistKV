#include "net/connection.h"

#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <string_view>

#include "protocol/resp_writer.h"

namespace kv::net {

namespace {
constexpr size_t kReadChunk = 16 * 1024;
}

Connection::Connection(int fd, const CommandHandlerFn& handler) : fd_(fd), handler_(handler) {}

Connection::~Connection() { ::close(fd_); }

bool Connection::onReadable() {
  // One read per readiness event. epoll is level-triggered, so if more data is
  // waiting we are woken again, after other connections have had a turn.
  char buf[kReadChunk];
  ssize_t n = ::read(fd_, buf, sizeof(buf));
  if (n == 0) return false;  // peer closed
  if (n < 0) return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR;

  in_.append(buf, static_cast<size_t>(n));
  return processAndFlush();
}

bool Connection::onWritable() { return processAndFlush(); }

bool Connection::processAndFlush() {
  // Parsing pauses at the high-water mark. If a flush then drains the output
  // completely, no EPOLLOUT event will follow, and if the client has finished
  // sending no EPOLLIN will either, so buffered commands must be resumed
  // here rather than waiting for an event that may never come.
  while (true) {
    size_t buffered = in_.size();
    if (!processInput()) {
      flush();  // best effort: deliver the protocol error before closing
      return false;
    }
    if (!flush()) return false;
    bool no_progress = in_.size() == buffered;  // empty, or only a partial command
    if (in_.empty() || no_progress || pendingOutput() >= kOutputHighWater) return true;
  }
}

uint32_t Connection::wantedEvents() const {
  uint32_t events = 0;
  if (pendingOutput() < kOutputHighWater) events |= EPOLLIN;
  if (pendingOutput() > 0) events |= EPOLLOUT;
  return events;
}

bool Connection::processInput() {
  std::string_view input(in_);
  size_t offset = 0;
  while (pendingOutput() < kOutputHighWater) {
    resp::Command cmd;
    resp::Parser::Result result = parser_.parse(input.substr(offset), cmd);
    offset += result.consumed;
    if (result.status == resp::Parser::Status::kNeedMore) break;
    if (result.status == resp::Parser::Status::kError) {
      resp::appendError(out_, "ERR " + parser_.error());
      in_.clear();
      return false;
    }
    handler_(cmd, out_);
  }
  in_.erase(0, offset);
  return true;
}

bool Connection::flush() {
  while (pendingOutput() > 0) {
    // MSG_NOSIGNAL: a peer that has gone away gives EPIPE, not SIGPIPE.
    ssize_t n = ::send(fd_, out_.data() + out_sent_, pendingOutput(), MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) break;
      return false;
    }
    out_sent_ += static_cast<size_t>(n);
  }
  if (pendingOutput() == 0) {
    out_.clear();
    out_sent_ = 0;
  }
  return true;
}

}  // namespace kv::net
