#include <gtest/gtest.h>
#include "db.hpp"
#include "bloomfilter.hpp"
#include "internal_key.hpp"
#include "wal.hpp"
#include "memtable.hpp"
#include <filesystem>
#include <thread>
#include <chrono>
#include <vector>
#include <string>
#include <atomic>

// ─── helpers ────────────────────────────────────────────────────────────────

static std::vector<uint8_t> Bytes(const std::string& s) {
    return std::vector<uint8_t>(s.begin(), s.end());
}
static std::string Str(const std::vector<uint8_t>& v) {
    return std::string(v.begin(), v.end());
}

// RAII directory cleaner
struct TempDir {
    std::string path;
    explicit TempDir(const std::string& p) : path(p) {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// ─── 1. Basic Put / Get ──────────────────────────────────────────────────────

TEST(TitanKV, BasicPutGet) {
    TempDir d("tk_basic");
    auto db = DB::Open(d.path);
    ASSERT_NE(db, nullptr);

    ASSERT_TRUE(db->Put("hello", Bytes("world")));
    ASSERT_TRUE(db->Put("foo",   Bytes("bar")));

    std::vector<uint8_t> val;
    bool exists = false;

    ASSERT_TRUE(db->Get("hello", val, exists));
    EXPECT_TRUE(exists);
    EXPECT_EQ(Str(val), "world");

    ASSERT_TRUE(db->Get("foo", val, exists));
    EXPECT_TRUE(exists);
    EXPECT_EQ(Str(val), "bar");

    db->Close();
}

// ─── 2. Get Missing Key ──────────────────────────────────────────────────────

TEST(TitanKV, MissingKey) {
    TempDir d("tk_missing");
    auto db = DB::Open(d.path);
    ASSERT_NE(db, nullptr);

    std::vector<uint8_t> val;
    bool exists = true; // intentionally true to verify it gets set false
    ASSERT_TRUE(db->Get("nonexistent", val, exists));
    EXPECT_FALSE(exists);

    db->Close();
}

// ─── 3. Overwrite (latest value wins) ───────────────────────────────────────

TEST(TitanKV, OverwriteValue) {
    TempDir d("tk_overwrite");
    auto db = DB::Open(d.path);
    ASSERT_NE(db, nullptr);

    ASSERT_TRUE(db->Put("key", Bytes("v1")));
    ASSERT_TRUE(db->Put("key", Bytes("v2")));
    ASSERT_TRUE(db->Put("key", Bytes("v3")));

    std::vector<uint8_t> val;
    bool exists = false;
    ASSERT_TRUE(db->Get("key", val, exists));
    EXPECT_TRUE(exists);
    EXPECT_EQ(Str(val), "v3");

    db->Close();
}

// ─── 4. Delete ───────────────────────────────────────────────────────────────

TEST(TitanKV, DeleteKey) {
    TempDir d("tk_delete");
    auto db = DB::Open(d.path);
    ASSERT_NE(db, nullptr);

    ASSERT_TRUE(db->Put("k", Bytes("v")));
    ASSERT_TRUE(db->Delete("k"));

    std::vector<uint8_t> val;
    bool exists = true;
    ASSERT_TRUE(db->Get("k", val, exists));
    EXPECT_FALSE(exists);

    db->Close();
}

// ─── 5. Delete then re-Put ───────────────────────────────────────────────────

TEST(TitanKV, DeleteThenRePut) {
    TempDir d("tk_del_reput");
    auto db = DB::Open(d.path);
    ASSERT_NE(db, nullptr);

    ASSERT_TRUE(db->Put("k", Bytes("original")));
    ASSERT_TRUE(db->Delete("k"));
    ASSERT_TRUE(db->Put("k", Bytes("reborn")));

    std::vector<uint8_t> val;
    bool exists = false;
    ASSERT_TRUE(db->Get("k", val, exists));
    EXPECT_TRUE(exists);
    EXPECT_EQ(Str(val), "reborn");

    db->Close();
}

// ─── 6. WAL Recovery ─────────────────────────────────────────────────────────

TEST(TitanKV, WALRecovery) {
    TempDir d("tk_recovery");

    // Write data then close WITHOUT explicit Close() to simulate crash-like
    // (destructor calls Close() which is fine for WAL flushing)
    {
        auto db = DB::Open(d.path);
        ASSERT_NE(db, nullptr);
        ASSERT_TRUE(db->Put("persist1", Bytes("aaa")));
        ASSERT_TRUE(db->Put("persist2", Bytes("bbb")));
        ASSERT_TRUE(db->Put("persist1", Bytes("ccc"))); // overwrite
        db->Close();
    }

    // Reopen — data must survive via WAL or SSTable
    {
        auto db = DB::Open(d.path);
        ASSERT_NE(db, nullptr);

        std::vector<uint8_t> val;
        bool exists = false;

        ASSERT_TRUE(db->Get("persist1", val, exists));
        EXPECT_TRUE(exists);
        EXPECT_EQ(Str(val), "ccc");

        ASSERT_TRUE(db->Get("persist2", val, exists));
        EXPECT_TRUE(exists);
        EXPECT_EQ(Str(val), "bbb");

        db->Close();
    }
}

// ─── 7. Time-Travel (GetAt) ──────────────────────────────────────────────────

TEST(TitanKV, TimeTravel) {
    TempDir d("tk_timetravel");
    auto db = DB::Open(d.path);
    ASSERT_NE(db, nullptr);

    std::string key = "status";
    std::vector<std::string> versions = {"offline", "online", "busy", "away"};
    std::vector<uint64_t> seqAfter(versions.size());

    for (size_t i = 0; i < versions.size(); i++) {
        ASSERT_TRUE(db->Put(key, Bytes(versions[i])));
        seqAfter[i] = db->GetNextSeqNum();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    for (size_t i = 0; i < versions.size(); i++) {
        std::vector<uint8_t> val;
        bool exists = false;
        ASSERT_TRUE(db->GetAt(key, seqAfter[i], val, exists));
        ASSERT_TRUE(exists) << "version " << i << " missing";
        EXPECT_EQ(Str(val), versions[i]) << "version " << i << " mismatch";
    }

    db->Close();
}

// ─── 8. Many Keys ────────────────────────────────────────────────────────────

TEST(TitanKV, ManyKeys) {
    TempDir d("tk_manykeys");
    auto db = DB::Open(d.path);
    ASSERT_NE(db, nullptr);

    const int N = 200;
    for (int i = 0; i < N; i++) {
        std::string k = "key_" + std::to_string(i);
        std::string v = "val_" + std::to_string(i);
        ASSERT_TRUE(db->Put(k, Bytes(v)));
    }

    for (int i = 0; i < N; i++) {
        std::string k = "key_" + std::to_string(i);
        std::string expected = "val_" + std::to_string(i);
        std::vector<uint8_t> val;
        bool exists = false;
        ASSERT_TRUE(db->Get(k, val, exists)) << "Get failed for " << k;
        EXPECT_TRUE(exists)                  << "Key missing: " << k;
        EXPECT_EQ(Str(val), expected)        << "Wrong value for " << k;
    }

    db->Close();
}

// ─── 9. Flush + Read (force flush by writing past memtable threshold) ─────────

TEST(TitanKV, FlushThenRead) {
    TempDir d("tk_flush");
    auto db = DB::Open(d.path);
    ASSERT_NE(db, nullptr);
    db->SetMaxMemTableSize(1024); // tiny threshold to force flush

    std::string key = "flush_test";
    for (int i = 0; i < 40; i++) {
        std::string v = "value_" + std::to_string(i);
        ASSERT_TRUE(db->Put(key, Bytes(v)));
    }
    // allow flush to complete
    std::this_thread::sleep_for(std::chrono::seconds(2));

    std::vector<uint8_t> val;
    bool exists = false;
    ASSERT_TRUE(db->Get(key, val, exists));
    EXPECT_TRUE(exists);
    EXPECT_EQ(Str(val), "value_39");

    db->Close();
}

// ─── 10. Compaction correctness ──────────────────────────────────────────────

TEST(TitanKV, CompactionPurge) {
    TempDir d("tk_compaction");
    auto db = DB::Open(d.path);
    ASSERT_NE(db, nullptr);
    db->SetMaxMemTableSize(512);

    const std::string key = "compact_key";
    for (int i = 0; i < 60; i++) {
        std::string v = "old_" + std::to_string(i);
        ASSERT_TRUE(db->Put(key, Bytes(v)));
    }

    std::this_thread::sleep_for(std::chrono::seconds(4));

    std::vector<uint8_t> val;
    bool exists = false;
    ASSERT_TRUE(db->Get(key, val, exists));
    EXPECT_TRUE(exists);
    EXPECT_EQ(Str(val), "old_59");

    db->Close();
}

// ─── 11. Concurrent Writes ───────────────────────────────────────────────────

TEST(TitanKV, ConcurrentWrites) {
    TempDir d("tk_concurrent");
    auto db = DB::Open(d.path);
    ASSERT_NE(db, nullptr);

    std::atomic<int> failures{0};
    const int THREADS = 4;
    const int OPS_PER_THREAD = 50;
    std::vector<std::thread> threads;

    for (int t = 0; t < THREADS; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < OPS_PER_THREAD; i++) {
                std::string k = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                std::string v = "val_" + std::to_string(i);
                if (!db->Put(k, Bytes(v))) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& th : threads) th.join();

    EXPECT_EQ(failures.load(), 0) << "Some concurrent writes failed";

    // Spot-check a few keys
    for (int t = 0; t < THREADS; t++) {
        std::string k = "thread_" + std::to_string(t) + "_key_0";
        std::vector<uint8_t> val;
        bool exists = false;
        ASSERT_TRUE(db->Get(k, val, exists));
        EXPECT_TRUE(exists) << "Key missing after concurrent write: " << k;
    }

    db->Close();
}

// ─── 12. Empty Value ─────────────────────────────────────────────────────────

TEST(TitanKV, EmptyValue) {
    TempDir d("tk_emptyval");
    auto db = DB::Open(d.path);
    ASSERT_NE(db, nullptr);

    ASSERT_TRUE(db->Put("empty_key", {}));

    std::vector<uint8_t> val;
    bool exists = false;
    ASSERT_TRUE(db->Get("empty_key", val, exists));
    EXPECT_TRUE(exists);
    EXPECT_TRUE(val.empty());

    db->Close();
}

// ─── 13. Large Value ─────────────────────────────────────────────────────────

TEST(TitanKV, LargeValue) {
    TempDir d("tk_largeval");
    auto db = DB::Open(d.path);
    ASSERT_NE(db, nullptr);

    std::vector<uint8_t> big(64 * 1024, 0xAB); // 64 KB
    ASSERT_TRUE(db->Put("big", big));

    std::vector<uint8_t> val;
    bool exists = false;
    ASSERT_TRUE(db->Get("big", val, exists));
    EXPECT_TRUE(exists);
    EXPECT_EQ(val, big);

    db->Close();
}

// ─── 14. BloomFilter unit tests ──────────────────────────────────────────────

TEST(BloomFilter, BasicAddContains) {
    BloomFilter bf(4096, 3);
    bf.Add("hello");
    bf.Add("world");
    bf.Add("titankv");

    EXPECT_TRUE(bf.MightContain("hello"));
    EXPECT_TRUE(bf.MightContain("world"));
    EXPECT_TRUE(bf.MightContain("titankv"));

    // These should almost certainly be false with this filter size
    EXPECT_FALSE(bf.MightContain("definitely_not_here_xyzzy"));
}

TEST(BloomFilter, HasBitsAfterAdd) {
    BloomFilter bf(4096, 3);
    bf.Add("x");
    const auto& bytes = bf.Bytes();
    bool hasBits = false;
    for (uint8_t b : bytes) if (b) { hasBits = true; break; }
    EXPECT_TRUE(hasBits);
}

// ─── 15. InternalKey encoding / comparison ───────────────────────────────────

TEST(InternalKey, EncodeDecodeRoundTrip) {
    std::vector<uint8_t> userKey = {'a', 'b', 'c'};
    uint64_t seq = 42;
    uint8_t type = TypePut;

    auto encoded = EncodeInternalKey(userKey, seq, type);

    std::vector<uint8_t> outKey;
    uint64_t outSeq = 0;
    uint8_t outType = 0;
    ASSERT_TRUE(ParseInternalKey(encoded, outKey, outSeq, outType));

    EXPECT_EQ(outKey, userKey);
    EXPECT_EQ(outSeq, seq);
    EXPECT_EQ(outType, type);
}

TEST(InternalKey, DifferentUserKeysOrder) {
    auto ik1 = EncodeInternalKey({'a'}, 10, TypePut);
    auto ik2 = EncodeInternalKey({'b'}, 10, TypePut);
    EXPECT_LT(CompareInternalKeys(ik1, ik2), 0);
    EXPECT_GT(CompareInternalKeys(ik2, ik1), 0);
}

TEST(InternalKey, SameUserKeySeqDescending) {
    auto ikHigh = EncodeInternalKey({'k'}, 100, TypePut);
    auto ikLow  = EncodeInternalKey({'k'},  10, TypePut);
    // higher seq should sort first (< 0)
    EXPECT_LT(CompareInternalKeys(ikHigh, ikLow), 0);
    EXPECT_GT(CompareInternalKeys(ikLow, ikHigh), 0);
}

TEST(InternalKey, DeleteTypeComparison) {
    auto ikPut = EncodeInternalKey({'k'}, 5, TypePut);
    auto ikDel = EncodeInternalKey({'k'}, 5, TypeDelete);
    // Put (1) > Delete (0), so Put sorts first (< 0 result)
    EXPECT_LT(CompareInternalKeys(ikPut, ikDel), 0);
}

// ─── 16. WAL write + recover ─────────────────────────────────────────────────

TEST(WAL, WriteAndRecover) {
    TempDir d("tk_wal_unit");
    std::string walPath = d.path + "/test.wal";
    std::filesystem::create_directories(d.path);

    auto ik1 = EncodeInternalKey(Bytes("key1"), 1, TypePut);
    auto ik2 = EncodeInternalKey(Bytes("key2"), 2, TypePut);
    auto ik3 = EncodeInternalKey(Bytes("key1"), 3, TypeDelete);

    {
        auto wal = WAL::Open(walPath);
        ASSERT_NE(wal, nullptr);
        ASSERT_TRUE(wal->WriteRecord(ik1, Bytes("v1")));
        ASSERT_TRUE(wal->WriteRecord(ik2, Bytes("v2")));
        ASSERT_TRUE(wal->WriteRecord(ik3, {}));
        wal->Close();
    }

    // Recover into a fresh MemTable
    {
        auto wal = WAL::Open(walPath);
        ASSERT_NE(wal, nullptr);
        MemTable mt;
        uint64_t maxSeq = wal->Recover(&mt);
        EXPECT_EQ(maxSeq, 3u);

        std::vector<uint8_t> val;
        bool isDeleted = false;

        // key1 latest should be deleted (seq 3)
        bool found1 = mt.Get(Bytes("key1"), UINT64_MAX, val, isDeleted);
        EXPECT_TRUE(found1);
        EXPECT_TRUE(isDeleted);

        // key2 should be present
        bool found2 = mt.Get(Bytes("key2"), UINT64_MAX, val, isDeleted);
        EXPECT_TRUE(found2);
        EXPECT_FALSE(isDeleted);
        EXPECT_EQ(Str(val), "v2");

        wal->Close();
    }
}
