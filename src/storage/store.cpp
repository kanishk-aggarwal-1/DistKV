#include "storage/store.h"

#include <mutex>
#include <stdexcept>

namespace kv {

Store::Store(size_t num_stripes)
    : num_stripes_(num_stripes), stripes_(std::make_unique<Stripe[]>(num_stripes)) {
  if (num_stripes == 0) throw std::invalid_argument("Store needs at least one stripe");
}

Store::Stripe& Store::stripeFor(std::string_view key) const {
  // The per-stripe maps reuse the same hash, but libstdc++ sizes its bucket
  // arrays to prime numbers, so bucket choice is not correlated with this modulo.
  return stripes_[Hash{}(key) % num_stripes_];
}

std::optional<std::string> Store::get(std::string_view key) const {
  Stripe& stripe = stripeFor(key);
  std::shared_lock lock(stripe.mutex);
  auto it = stripe.map.find(key);
  if (it == stripe.map.end()) return std::nullopt;
  // Copy while locked: the entry may be overwritten as soon as we unlock.
  return it->second;
}

void Store::set(std::string_view key, std::string_view value) {
  Stripe& stripe = stripeFor(key);
  std::unique_lock lock(stripe.mutex);
  auto it = stripe.map.find(key);
  if (it != stripe.map.end()) {
    it->second.assign(value);
  } else {
    stripe.map.emplace(std::string(key), std::string(value));
  }
}

bool Store::del(std::string_view key) {
  Stripe& stripe = stripeFor(key);
  std::unique_lock lock(stripe.mutex);
  auto it = stripe.map.find(key);
  if (it == stripe.map.end()) return false;
  stripe.map.erase(it);
  return true;
}

size_t Store::size() const {
  size_t total = 0;
  for (size_t i = 0; i < num_stripes_; ++i) {
    std::shared_lock lock(stripes_[i].mutex);
    total += stripes_[i].map.size();
  }
  return total;
}

}  // namespace kv
