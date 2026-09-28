#include "storage/store.h"

#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

namespace kv {
namespace {

TEST(Store, GetMissingKey) {
  Store store;
  EXPECT_FALSE(store.get("missing").has_value());
}

TEST(Store, SetThenGet) {
  Store store;
  store.set("k", "v");
  EXPECT_EQ(store.get("k"), "v");
}

TEST(Store, SetOverwrites) {
  Store store;
  store.set("k", "v1");
  store.set("k", "v2");
  EXPECT_EQ(store.get("k"), "v2");
  EXPECT_EQ(store.size(), 1u);
}

TEST(Store, DelReportsWhetherKeyExisted) {
  Store store;
  store.set("k", "v");
  EXPECT_TRUE(store.del("k"));
  EXPECT_FALSE(store.del("k"));
  EXPECT_FALSE(store.get("k").has_value());
}

TEST(Store, KeysAndValuesAreBinarySafe) {
  Store store;
  std::string key("a\0b", 3);
  std::string value("\r\n\0", 3);
  store.set(key, value);
  EXPECT_EQ(store.get(key), value);
  EXPECT_FALSE(store.get("a").has_value());
}

TEST(Store, SingleStripeStillWorks) {
  Store store(1);
  for (int i = 0; i < 100; ++i) store.set(std::to_string(i), std::to_string(i * 2));
  EXPECT_EQ(store.size(), 100u);
  EXPECT_EQ(store.get("42"), "84");
}

TEST(Store, ZeroStripesIsRejected) { EXPECT_THROW(Store(0), std::invalid_argument); }

// Each thread owns a disjoint set of keys, so the final state is fully
// determined even though the threads run concurrently on shared stripes.
TEST(Store, ConcurrentWritersOnDisjointKeys) {
  Store store(8);  // few stripes, so threads collide on locks
  constexpr int kThreads = 8;
  constexpr int kKeysPerThread = 2000;

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&store, t] {
      for (int i = 0; i < kKeysPerThread; ++i) {
        std::string key = "t" + std::to_string(t) + ":" + std::to_string(i);
        store.set(key, "first");
        store.set(key, key);  // overwrite
        ASSERT_EQ(store.get(key), key);
        if (i % 2 == 0) ASSERT_TRUE(store.del(key));
      }
    });
  }
  for (auto& th : threads) th.join();

  EXPECT_EQ(store.size(), static_cast<size_t>(kThreads * kKeysPerThread / 2));
  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kKeysPerThread; ++i) {
      std::string key = "t" + std::to_string(t) + ":" + std::to_string(i);
      if (i % 2 == 0) {
        EXPECT_FALSE(store.get(key).has_value()) << key;
      } else {
        EXPECT_EQ(store.get(key), key);
      }
    }
  }
}

// Readers and writers hammer the same few keys. Mostly a target for
// ThreadSanitizer; the functional check is that a reader only ever sees a
// value some writer actually wrote.
TEST(Store, ConcurrentReadersAndWritersOnSameKeys) {
  Store store(4);
  constexpr int kIterations = 20000;
  const std::vector<std::string> keys = {"a", "b", "c"};

  std::vector<std::thread> threads;
  for (int w = 0; w < 2; ++w) {
    threads.emplace_back([&, w] {
      for (int i = 0; i < kIterations; ++i) {
        const std::string& key = keys[i % keys.size()];
        store.set(key, "writer" + std::to_string(w));
        if (i % 7 == 0) store.del(key);
      }
    });
  }
  for (int r = 0; r < 4; ++r) {
    threads.emplace_back([&] {
      for (int i = 0; i < kIterations; ++i) {
        auto value = store.get(keys[i % keys.size()]);
        if (value) ASSERT_TRUE(*value == "writer0" || *value == "writer1") << *value;
      }
    });
  }
  for (auto& th : threads) th.join();
}

}  // namespace
}  // namespace kv
