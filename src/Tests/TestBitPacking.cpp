#include <gtest/gtest.h>
#include <cstdint>
#include <sstream>
#include <vector>

#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"
#include "engine/data_storage/compression/bit_packing.h"

namespace {
template <typename T>
std::vector<T> RoundTrip(const std::vector<T>& data) {
    std::stringstream stream;
    Writer writer(stream);
    bit_packing::Encode(writer, data);

    Reader reader(stream);
    std::vector<T> out;
    bit_packing::Decode(reader, out);
    return out;
}
}

TEST(BitPacking, BitsNeeded) {
    using bit_packing::transform_helper::BitsNeeded;
    EXPECT_EQ(BitsNeeded(0), 0);
    EXPECT_EQ(BitsNeeded(1), 1);
    EXPECT_EQ(BitsNeeded(2), 2);
    EXPECT_EQ(BitsNeeded(3), 2);
    EXPECT_EQ(BitsNeeded(255), 8);
    EXPECT_EQ(BitsNeeded(256), 9);
}

TEST(BitPacking, Int64SmallRange) {
    std::vector<int64_t> data{100, 101, 100, 105, 102};
    EXPECT_EQ(RoundTrip(data), data);
}

TEST(BitPacking, Int64Negatives) {
    std::vector<int64_t> data{-5, -3, -10, 0, 7};
    EXPECT_EQ(RoundTrip(data), data);
}

TEST(BitPacking, AllEqualValuesZeroBits) {
    std::vector<int64_t> data{42, 42, 42, 42};
    EXPECT_EQ(RoundTrip(data), data);
}

TEST(BitPacking, EmptyVector) {
    std::vector<int64_t> data;
    EXPECT_EQ(RoundTrip(data), data);
}

TEST(BitPacking, Int32) {
    std::vector<int32_t> data{1000, 1005, 999, 1001};
    EXPECT_EQ(RoundTrip(data), data);
}

TEST(BitPacking, Int16) {
    std::vector<int16_t> data{10, 20, 15, 12};
    EXPECT_EQ(RoundTrip(data), data);
}

TEST(BitPacking, Int8) {
    std::vector<int8_t> data{1, 2, 3, 2, 1};
    EXPECT_EQ(RoundTrip(data), data);
}

TEST(BitPacking, FullRangeFallsBackToRaw) {
    std::vector<int64_t> data{INT64_MIN, INT64_MAX, 0};
    EXPECT_EQ(RoundTrip(data), data);
}

TEST(BitPacking, UnsupportedTypeUsesRaw) {
    std::vector<double> data{1.5, 2.5, -3.25};
    EXPECT_EQ(RoundTrip(data), data);
}
