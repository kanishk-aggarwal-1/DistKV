#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace kv {

// Thread-safe in-memory key-value map using lock striping.
//
// The keyspace is split into a fixed number of stripes by key hash. Each
// stripe is an independent hash map guarded by its own reader-writer lock,
// so operations on keys in different stripes never contend. Locks are held
// only for the map operation itself, never across I/O.
class Store {
 public:
  explicit Store(size_t num_stripes = 256);

  std::optional<std::string> get(std::string_view key) const;
  void set(std::string_view key, std::string_view value);
  // Returns true if the key existed.
  bool del(std::string_view key);

  // Total number of keys. Not a consistent snapshot while writers are active.
  size_t size() const;

 private:
  // Lets the maps look up a std::string_view without building a std::string.
  struct Hash {
    using is_transparent = void;
    size_t operator()(std::string_view s) const { return std::hash<std::string_view>{}(s); }
  };

  // Aligned to a cache line so that neighbouring stripes' locks do not
  // share one (false sharing).
  struct alignas(64) Stripe {
    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, std::string, Hash, std::equal_to<>> map;
  };

  Stripe& stripeFor(std::string_view key) const;

  size_t num_stripes_;
  std::unique_ptr<Stripe[]> stripes_;
};

}  // namespace kv
