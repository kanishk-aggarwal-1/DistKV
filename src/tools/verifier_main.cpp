// distkv-verifier: checks that no acknowledged write is lost when a node
// fails, and measures how long writes to the failed node's slots are
// unavailable.
//
//   distkv-verifier --seeds HOST:PORT[,HOST:PORT...] --duration SEC
//                   [--writers N] [--prefix P]
//                   [--victim HOST:PORT --kill-after SEC --kill-cmd 'COMMAND']
//                   [--json FILE] [--timeline FILE]
//
// Writers SET unique keys (prefix:writer:seq = seq) through cluster-aware
// clients and record each outcome:
//   acked      "+OK": the write must survive
//   refused    "-NOREPLICAS Not enough good replicas": not applied
//   ambiguous  lost connection, or "not acknowledged by the backup": may or
//              may not have been applied
//   error      anything else (not applied)
// With --victim, two probe threads write about once per millisecond: one only
// to slots the victim serves, one only to slots it does not (the "bystander"
// probe, which shows whether the rest of the cluster stayed available; the
// general writers cannot show that, since a synchronous client that picks a
// victim key blocks until it is served again). --kill-cmd is run
// --kill-after seconds in. The kill time and every
// write time come from this process's clock.
//
// Afterwards every acknowledged key is read back. Exit status 1 if any is
// missing or has the wrong value.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "client/resp_client.h"
#include "cluster/slot.h"

namespace {

using Clock = std::chrono::steady_clock;
using kv::client::ClusterClient;
using kv::client::Reply;

enum class Outcome { kAcked, kRefused, kAmbiguous, kError };

struct Options {
  std::vector<std::string> seeds;
  int duration_s = 30;
  int writers = 8;
  std::string prefix = "verify";
  std::string victim;
  double kill_after_s = -1;
  std::string kill_cmd;
  std::string json_path;
  std::string timeline_path;
};

struct Record {
  int64_t t_ms;  // since start
  Outcome outcome;
  bool victim_slot;
  bool bystander_probe;  // written by the bystander probe
  bool victim_probe;     // written by the victim probe
};

struct WriterLog {
  std::vector<Record> records;
  std::vector<std::pair<std::string, std::string>> acked;
  std::vector<std::pair<std::string, std::string>> ambiguous;
  std::map<std::string, int> error_samples;
};

std::vector<std::string> split(const std::string& s, char sep) {
  std::vector<std::string> out;
  size_t start = 0;
  while (start <= s.size()) {
    size_t end = s.find(sep, start);
    if (end == std::string::npos) end = s.size();
    if (end > start) out.push_back(s.substr(start, end - start));
    start = end + 1;
  }
  return out;
}

void usage(const char* prog) {
  std::fprintf(stderr,
               "usage: %s --seeds HOST:PORT[,...] --duration SEC [--writers N] [--prefix P]\n"
               "          [--victim HOST:PORT --kill-after SEC --kill-cmd CMD]\n"
               "          [--json FILE] [--timeline FILE]\n",
               prog);
}

bool parseOptions(int argc, char** argv, Options& o) {
  for (int i = 1; i < argc; ++i) {
    std::string flag = argv[i];
    if (i + 1 >= argc) return false;
    std::string value = argv[++i];
    try {
      if (flag == "--seeds") o.seeds = split(value, ',');
      else if (flag == "--duration") o.duration_s = std::stoi(value);
      else if (flag == "--writers") o.writers = std::stoi(value);
      else if (flag == "--prefix") o.prefix = value;
      else if (flag == "--victim") o.victim = value;
      else if (flag == "--kill-after") o.kill_after_s = std::stod(value);
      else if (flag == "--kill-cmd") o.kill_cmd = value;
      else if (flag == "--json") o.json_path = value;
      else if (flag == "--timeline") o.timeline_path = value;
      else return false;
    } catch (const std::exception&) {
      return false;
    }
  }
  if (o.seeds.empty() || o.duration_s <= 0 || o.writers <= 0) return false;
  if (!o.victim.empty() && (o.kill_after_s < 0 || o.kill_cmd.empty())) return false;
  return true;
}

Outcome classify(const Reply& reply) {
  if (reply.type == '+') return Outcome::kAcked;
  if (reply.type == ClusterClient::kAmbiguous) return Outcome::kAmbiguous;
  if (reply.isErrorWithPrefix("NOREPLICAS Not enough good replicas")) return Outcome::kRefused;
  if (reply.isErrorWithPrefix("NOREPLICAS Write not acknowledged")) return Outcome::kAmbiguous;
  return Outcome::kError;
}

// Slots currently served by `victim` (HOST:PORT), from CLUSTER SLOTS.
std::set<uint16_t> slotsOf(const std::vector<std::string>& seeds, const std::string& victim) {
  for (const std::string& seed : seeds) {
    try {
      kv::client::RespClient conn(seed);
      Reply slots = conn.command({"CLUSTER", "SLOTS"});
      if (slots.type != '*') continue;
      std::set<uint16_t> result;
      for (const Reply& range : slots.elements) {
        const Reply& node = range.elements.at(2);
        std::string addr = node.elements.at(0).str + ":" + std::to_string(node.elements.at(1).integer);
        if (addr != victim) continue;
        for (int64_t s = range.elements.at(0).integer; s <= range.elements.at(1).integer; ++s) {
          result.insert(static_cast<uint16_t>(s));
        }
      }
      return result;
    } catch (const std::exception&) {
    }
  }
  return {};
}

int64_t sinceMs(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count();
}

int64_t wallMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// One writer (or probe). `slots`, if given, restricts keys to those slots,
// or with `exclude` to every other slot.
void writeLoop(const Options& o, const std::string& name, const std::set<uint16_t>* slots,
               bool exclude, const std::set<uint16_t>& victim_slots, Clock::time_point start,
               const std::atomic<bool>& stop, WriterLog& log) {
  ClusterClient client(o.seeds);
  uint64_t seq = 0;
  uint64_t candidate = 0;
  while (!stop.load()) {
    std::string key;
    if (slots != nullptr) {
      // Find the next key that hashes into a wanted slot.
      do {
        key = o.prefix + ":" + name + ":" + std::to_string(candidate++);
      } while ((slots->count(kv::cluster::keySlot(key)) > 0) == exclude);
    } else {
      key = o.prefix + ":" + name + ":" + std::to_string(seq);
    }
    std::string value = std::to_string(seq++);
    Outcome outcome;
    Reply reply;
    try {
      reply = client.execute({"SET", key, value});
      outcome = classify(reply);
    } catch (const std::exception& e) {
      // Only redirect loops end up here: every reply was a redirect, so the
      // write was not applied anywhere.
      outcome = Outcome::kError;
      reply.str = e.what();
    }
    bool in_victim = victim_slots.count(kv::cluster::keySlot(key)) > 0;
    log.records.push_back({sinceMs(start), outcome, in_victim, slots != nullptr && exclude,
                           slots != nullptr && !exclude});
    switch (outcome) {
      case Outcome::kAcked:
        log.acked.emplace_back(std::move(key), std::move(value));
        break;
      case Outcome::kAmbiguous:
        log.ambiguous.emplace_back(std::move(key), std::move(value));
        break;
      case Outcome::kError:
        ++log.error_samples[reply.str.substr(0, 60)];
        break;
      case Outcome::kRefused:
        break;
    }
    if (slots != nullptr) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

}  // namespace

int main(int argc, char** argv) {
  Options o;
  if (!parseOptions(argc, argv, o)) {
    usage(argv[0]);
    return 2;
  }

  std::set<uint16_t> victim_slots;
  if (!o.victim.empty()) {
    victim_slots = slotsOf(o.seeds, o.victim);
    if (victim_slots.empty()) {
      std::fprintf(stderr, "victim %s serves no slots (check the address)\n", o.victim.c_str());
      return 2;
    }
    std::printf("victim %s serves %zu slots\n", o.victim.c_str(), victim_slots.size());
  }

  const auto start = Clock::now();
  const int64_t start_wall = wallMs();
  std::atomic<bool> stop{false};
  std::vector<WriterLog> logs(o.writers + 2);  // the last two are the probes
  std::vector<std::thread> threads;
  for (int w = 0; w < o.writers; ++w) {
    threads.emplace_back([&, w] {
      writeLoop(o, "w" + std::to_string(w), nullptr, false, victim_slots, start, stop, logs[w]);
    });
  }
  if (!o.victim.empty()) {
    threads.emplace_back([&] {
      writeLoop(o, "probe", &victim_slots, false, victim_slots, start, stop, logs[o.writers]);
    });
    threads.emplace_back([&] {
      writeLoop(o, "bystander", &victim_slots, true, victim_slots, start, stop, logs[o.writers + 1]);
    });
  }

  std::optional<int64_t> kill_issued_ms, kill_returned_ms;
  int64_t kill_issued_wall = 0;
  if (!o.victim.empty()) {
    std::this_thread::sleep_until(start + std::chrono::milliseconds(
                                              static_cast<int64_t>(o.kill_after_s * 1000)));
    kill_issued_ms = sinceMs(start);
    kill_issued_wall = wallMs();
    int status = std::system(o.kill_cmd.c_str());
    kill_returned_ms = sinceMs(start);
    std::printf("kill command exited with %d after %lld ms\n", status,
                static_cast<long long>(*kill_returned_ms - *kill_issued_ms));
  }
  std::this_thread::sleep_until(start + std::chrono::seconds(o.duration_s));
  stop = true;
  for (auto& t : threads) t.join();
  const int64_t end_ms = sinceMs(start);

  // ---- Tally.
  std::map<Outcome, uint64_t> counts;
  std::vector<std::pair<std::string, std::string>> acked, ambiguous;
  std::map<std::string, int> errors;
  std::vector<Record> all;
  for (WriterLog& log : logs) {
    for (const Record& r : log.records) ++counts[r.outcome];
    all.insert(all.end(), log.records.begin(), log.records.end());
    acked.insert(acked.end(), log.acked.begin(), log.acked.end());
    ambiguous.insert(ambiguous.end(), log.ambiguous.begin(), log.ambiguous.end());
    for (const auto& [msg, n] : log.error_samples) errors[msg] += n;
  }
  std::sort(all.begin(), all.end(), [](const Record& a, const Record& b) { return a.t_ms < b.t_ms; });

  // Failover time: from issuing the kill to the first acknowledged write in
  // one of the victim's slots after it (the probe writes there every ~1 ms).
  // Also the longest gap between acknowledged writes to those slots.
  // The kill happened somewhere between issuing and the command returning, so
  // failover is reported from both ends: from issue (an upper bound) and from
  // return (a lower bound).
  std::optional<int64_t> failover_ms, failover_from_return_ms;
  int64_t longest_gap_ms = 0;
  int64_t longest_bystander_gap_ms = 0;
  if (kill_issued_ms) {
    std::optional<int64_t> last_ok, last_bystander_ok;
    for (const Record& r : all) {
      if (r.outcome != Outcome::kAcked) continue;
      if (r.bystander_probe) {
        if (last_bystander_ok) {
          longest_bystander_gap_ms = std::max(longest_bystander_gap_ms, r.t_ms - *last_bystander_ok);
        }
        last_bystander_ok = r.t_ms;
      }
      if (!r.victim_slot) continue;
      if (last_ok) longest_gap_ms = std::max(longest_gap_ms, r.t_ms - *last_ok);
      last_ok = r.t_ms;
      if (!failover_ms && r.t_ms > *kill_returned_ms) {
        failover_ms = r.t_ms - *kill_issued_ms;
        failover_from_return_ms = r.t_ms - *kill_returned_ms;
      }
    }
  }

  // ---- Verify: every acknowledged key must read back its value.
  std::printf("verifying %zu acknowledged and %zu ambiguous writes...\n", acked.size(),
              ambiguous.size());
  std::atomic<uint64_t> lost{0}, ambiguous_present{0};
  std::mutex samples_mutex;
  std::vector<std::string> lost_samples;
  const int readers = 8;
  std::vector<std::thread> verifiers;
  for (int r = 0; r < readers; ++r) {
    verifiers.emplace_back([&, r] {
      ClusterClient client(o.seeds);
      auto get = [&](const std::string& key) -> std::optional<std::string> {
        for (int attempt = 0; attempt < 5; ++attempt) {
          Reply reply = client.execute({"GET", key});
          if (reply.type == '$') return reply.is_null ? std::nullopt : std::optional(reply.str);
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return std::string("<read failed>");
      };
      for (size_t i = r; i < acked.size(); i += readers) {
        std::optional<std::string> got = get(acked[i].first);
        if (got != acked[i].second) {
          ++lost;
          std::lock_guard lock(samples_mutex);
          if (lost_samples.size() < 10) lost_samples.push_back(acked[i].first + " -> " + got.value_or("(nil)"));
        }
      }
      for (size_t i = r; i < ambiguous.size(); i += readers) {
        if (get(ambiguous[i].first).has_value()) ++ambiguous_present;
      }
    });
  }
  for (auto& t : verifiers) t.join();

  // ---- Report.
  const double seconds = end_ms / 1000.0;
  std::printf("\n%-28s %llu\n", "acknowledged writes", static_cast<unsigned long long>(counts[Outcome::kAcked]));
  std::printf("%-28s %llu\n", "refused (no synced backup)", static_cast<unsigned long long>(counts[Outcome::kRefused]));
  std::printf("%-28s %llu (%llu present afterwards)\n", "ambiguous",
              static_cast<unsigned long long>(counts[Outcome::kAmbiguous]),
              static_cast<unsigned long long>(ambiguous_present.load()));
  std::printf("%-28s %llu\n", "errors", static_cast<unsigned long long>(counts[Outcome::kError]));
  for (const auto& [msg, n] : errors) std::printf("    %6d x %s\n", n, msg.c_str());
  std::printf("%-28s %.0f/s over %.1f s\n", "acknowledged write rate", counts[Outcome::kAcked] / seconds, seconds);
  if (kill_issued_ms) {
    std::printf("%-28s %s\n", "failover (kill -> first ack)",
                failover_ms ? (std::to_string(*failover_from_return_ms) + "-" +
                               std::to_string(*failover_ms) + " ms")
                                  .c_str()
                            : "never within the run");
    std::printf("%-28s %lld ms\n", "longest gap in victim acks", static_cast<long long>(longest_gap_ms));
    std::printf("%-28s %lld ms\n", "longest gap, other groups",
                static_cast<long long>(longest_bystander_gap_ms));
  }
  std::printf("%-28s %llu\n", "LOST acknowledged writes", static_cast<unsigned long long>(lost.load()));
  for (const std::string& s : lost_samples) std::printf("    %s\n", s.c_str());

  if (!o.json_path.empty()) {
    std::ofstream json(o.json_path);
    json << "{\n"
         << "  \"start_wall_ms\": " << start_wall << ",\n"
         << "  \"duration_ms\": " << end_ms << ",\n"
         << "  \"writers\": " << o.writers << ",\n"
         << "  \"victim\": \"" << o.victim << "\",\n"
         << "  \"victim_slots\": " << victim_slots.size() << ",\n"
         << "  \"kill_issued_ms\": " << (kill_issued_ms ? std::to_string(*kill_issued_ms) : "null") << ",\n"
         << "  \"kill_returned_ms\": " << (kill_returned_ms ? std::to_string(*kill_returned_ms) : "null") << ",\n"
         << "  \"kill_issued_wall_ms\": " << kill_issued_wall << ",\n"
         << "  \"failover_ms\": " << (failover_ms ? std::to_string(*failover_ms) : "null") << ",\n"
         << "  \"failover_from_return_ms\": "
         << (failover_from_return_ms ? std::to_string(*failover_from_return_ms) : "null") << ",\n"
         << "  \"longest_victim_gap_ms\": " << longest_gap_ms << ",\n"
         << "  \"longest_bystander_gap_ms\": " << longest_bystander_gap_ms << ",\n"
         << "  \"acked\": " << counts[Outcome::kAcked] << ",\n"
         << "  \"refused\": " << counts[Outcome::kRefused] << ",\n"
         << "  \"ambiguous\": " << counts[Outcome::kAmbiguous] << ",\n"
         << "  \"ambiguous_present\": " << ambiguous_present.load() << ",\n"
         << "  \"errors\": " << counts[Outcome::kError] << ",\n"
         << "  \"lost\": " << lost.load() << "\n"
         << "}\n";
  }
  if (!o.timeline_path.empty()) {
    // Outcomes per 100 ms bucket, for charting throughput around the kill.
    std::ofstream csv(o.timeline_path);
    // victim_probe_acked and bystander_acked come from the two probes, which
    // write at the same pace, so they are directly comparable.
    csv << "t_ms,acked,refused,ambiguous,errors,victim_acked,bystander_acked,victim_probe_acked\n";
    const int64_t bucket = 100;
    std::vector<std::array<uint64_t, 7>> buckets(static_cast<size_t>(end_ms / bucket + 1));
    for (const Record& r : all) {
      auto& b = buckets[static_cast<size_t>(r.t_ms / bucket)];
      ++b[static_cast<size_t>(r.outcome)];
      if (r.victim_slot && r.outcome == Outcome::kAcked) ++b[4];
      if (r.bystander_probe && r.outcome == Outcome::kAcked) ++b[5];
      if (r.victim_probe && r.outcome == Outcome::kAcked) ++b[6];
    }
    for (size_t i = 0; i < buckets.size(); ++i) {
      csv << i * bucket;
      for (uint64_t v : buckets[i]) csv << ',' << v;
      csv << '\n';
    }
  }
  return lost.load() == 0 ? 0 : 1;
}
