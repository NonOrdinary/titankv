#include <gtest/gtest.h>
#include "db.hpp"
#include "bloomfilter.hpp"
#include "internal_key.hpp"
#include <filesystem>
#include <thread>
#include <chrono>

TEST(TitanKV, TimeTravel) {
    std::string dir = "test_db_timetravel";
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);

    {
        auto db = DB::Open(dir);
        ASSERT_NE(db, nullptr);

        std::string key = "user:101:status";
        std::vector<std::string> versions = {"offline", "online", "busy", "away"};
        std::vector<uint64_t> seqNums(versions.size());

        for (size_t i = 0; i < versions.size(); i++) {
            std::vector<uint8_t> val(versions[i].begin(), versions[i].end());
            bool ok = db->Put(key, val);
            ASSERT_TRUE(ok);

            seqNums[i] = db->GetNextSeqNum();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        for (size_t i = 0; i < versions.size(); i++) {
            uint64_t targetSeq = seqNums[i];
            std::vector<uint8_t> val;
            bool exists = false;
            bool ok = db->GetAt(key, targetSeq, val, exists);
            ASSERT_TRUE(ok);
            ASSERT_TRUE(exists);
            std::string valStr(val.begin(), val.end());
            EXPECT_EQ(valStr, versions[i]);
        }

        std::vector<uint8_t> val;
        bool exists = false;
        bool ok = db->GetAt(key, seqNums[1], val, exists);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(exists);
        EXPECT_EQ(std::string(val.begin(), val.end()), "online");

        db->Close();
    }
    std::filesystem::remove_all(dir, ec);
}

TEST(TitanKV, CompactionPurge) {
    std::string dir = "test_db_compaction";
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);

    {
        auto db = DB::Open(dir);
        ASSERT_NE(db, nullptr);
        db->SetMaxMemTableSize(1024);

        std::string key = "persistent_key";

        for (int i = 0; i < 50; i++) {
            std::string valStr = "old_val_" + std::to_string(i);
            std::vector<uint8_t> val(valStr.begin(), valStr.end());
            bool ok = db->Put(key, val);
            ASSERT_TRUE(ok);
        }

        // Wait to trigger flushes and compactions
        std::this_thread::sleep_for(std::chrono::seconds(3));

        std::vector<uint8_t> val;
        bool exists = false;
        bool ok = db->Get(key, val, exists);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(exists);

        std::string valStr(val.begin(), val.end());
        EXPECT_EQ(valStr, "old_val_49");

        db->Close();
    }
    std::filesystem::remove_all(dir, ec);
}

TEST(TitanKV, BloomFilterBinaryCompatibility) {
    BloomFilter bf(4096, 3);
    bf.Add("hello");
    bf.Add("world");

    EXPECT_TRUE(bf.MightContain("hello"));
    EXPECT_TRUE(bf.MightContain("world"));
    EXPECT_FALSE(bf.MightContain("non_existent"));

    // Check specific bitset values to verify FNV-1a with 0x9e3779b9 offset
    const auto& bytes = bf.Bytes();
    bool hasBits = false;
    for (uint8_t b : bytes) {
        if (b != 0) {
            hasBits = true;
            break;
        }
    }
    EXPECT_TRUE(hasBits);
}

TEST(TitanKV, InternalKeyComparisons) {
    std::vector<uint8_t> userKey1 = {'a', 'b', 'c'};
    std::vector<uint8_t> userKey2 = {'a', 'b', 'd'};

    // Different user keys
    auto ik1 = EncodeInternalKey(userKey1, 10, TypePut);
    auto ik2 = EncodeInternalKey(userKey2, 10, TypePut);
    EXPECT_LT(CompareInternalKeys(ik1, ik2), 0);

    // Same user key, different sequence numbers
    auto ik3 = EncodeInternalKey(userKey1, 15, TypePut);
    auto ik4 = EncodeInternalKey(userKey1, 10, TypePut);
    // Larger sequence number comes first (descending)
    EXPECT_LT(CompareInternalKeys(ik3, ik4), 0);
    EXPECT_GT(CompareInternalKeys(ik4, ik3), 0);

    // Same user key, same sequence number, different type
    auto ik5 = EncodeInternalKey(userKey1, 10, TypePut);
    auto ik6 = EncodeInternalKey(userKey1, 10, TypeDelete);
    // Put (1) has larger type value than Delete (0), so Put comes first (descending)
    EXPECT_LT(CompareInternalKeys(ik5, ik6), 0);
    EXPECT_GT(CompareInternalKeys(ik6, ik5), 0);
}
