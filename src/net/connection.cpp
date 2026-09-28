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

Connection::Connection(int fd, const CommandHandlerFn& handler, const ReplyGate* gate)
    : fd_(fd), handler_(handler), gate_(gate) {}

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

bool Connection::onReplicationProgress() {
  if (held_.empty()) return true;
  // Releasing replies may drop below the high-water mark, so buffered
  // commands may be able to run again too.
  return processAndFlush();
}

bool Connection::processAndFlush() {
  // Parsing pauses at the high-water mark. If a flush then drains the output
  // completely, no EPOLLOUT event will follow, and if the client has finished
  // sending no EPOLLIN will either, so buffered commands must be resumed
  // here rather than waiting for an event that may never come.
  while (true) {
    size_t buffered = in_.size();
    bool ok = processInput();
    releaseHeldReplies();
    if (!ok) {
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
  if (out_.size() > out_sent_) events |= EPOLLOUT;
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
      // Behind any held replies, like every other reply.
      std::string error;
      resp::appendError(error, "ERR " + parser_.error());
      if (held_.empty()) {
        out_ += error;
      } else {
        held_.back().trailing += error;
        held_bytes_ += error.size();
      }
      in_.clear();
      return false;
    }

    // The handler appends straight to out_; if this reply must be held (or
    // queue behind a held one) it is moved from the tail of out_.
    const size_t before = out_.size();
    session_.reply_seq = 0;
    handler_(cmd, session_, out_);
    if (session_.reply_seq != 0 || !held_.empty()) {
      std::string reply = out_.substr(before);
      out_.resize(before);
      held_bytes_ += reply.size();
      if (session_.reply_seq != 0) {
        held_.push_back({session_.reply_seq, std::move(reply), {}});
      } else {
        held_.back().trailing += reply;
      }
    }
  }
  in_.erase(0, offset);
  return true;
}

void Connection::releaseHeldReplies() {
  while (!held_.empty()) {
    HeldReply& front = held_.front();
    ReplyGate::Status status = gate_->status(front.seq);
    if (status == ReplyGate::Status::kWaiting) break;
    out_ += status == ReplyGate::Status::kReady ? front.reply : gate_->failed_reply;
    out_ += front.trailing;
    held_bytes_ -= front.reply.size() + front.trailing.size();
    held_.pop_front();
  }
}

bool Connection::flush() {
  while (out_.size() > out_sent_) {
    // MSG_NOSIGNAL: a peer that has gone away gives EPIPE, not SIGPIPE.
    ssize_t n = ::send(fd_, out_.data() + out_sent_, out_.size() - out_sent_, MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) break;
      return false;
    }
    out_sent_ += static_cast<size_t>(n);
  }
  if (out_sent_ == out_.size()) {
    out_.clear();
    out_sent_ = 0;
  }
  return true;
}

}  // namespace kv::net
