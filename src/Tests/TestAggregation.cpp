#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include "engine/data_storage/batch.h"
#include "engine/data_storage/schema.h"
#include "queries_executor/aggregation.h"
#include "utils.h"

namespace {
std::shared_ptr<Batch> MakeBatch(const Schema& schema, const std::vector<Row>& rows) {
    auto batch = std::make_shared<Batch>(schema, rows.size());
    for (const auto& row : rows) {
        batch->AddRow(Row(row));
    }
    return batch;
}

std::vector<uint32_t> SingleGroup(size_t n) {
    return std::vector<uint32_t>(n, 0);
}
}

TEST(Aggregation, Count) {
    Schema schema({"v"}, {Type::int64});
    auto batch = MakeBatch(schema, {{"1"}, {"2"}, {"3"}});

    CountAggregation agg;
    agg.RunBatch(batch, SingleGroup(3), 0);
    EXPECT_EQ(agg.GetResultValue(0), "3");
    EXPECT_EQ(agg.GetResultType(), Type::int64);
}

TEST(Aggregation, CountSkipsBannedSentinel) {
    Schema schema({"v"}, {Type::int64});
    auto batch = MakeBatch(schema, {{"1"}, {"2"}, {"3"}});

    std::vector<uint32_t> gi{0, UINT32_MAX, 0};  // middle row banned
    CountAggregation agg;
    agg.RunBatch(batch, gi, 0);
    EXPECT_EQ(agg.GetResultValue(0), "2");
}

TEST(Aggregation, Sum) {
    Schema schema({"v"}, {Type::int64});
    auto batch = MakeBatch(schema, {{"10"}, {"20"}, {"30"}});

    SumAggregation agg("v");
    agg.RunBatch(batch, SingleGroup(3), 0);
    EXPECT_EQ(agg.GetResultValue(0), "60");
    EXPECT_EQ(agg.GetResultType(), Type::int128);
}

TEST(Aggregation, Avg) {
    Schema schema({"v"}, {Type::int64});
    auto batch = MakeBatch(schema, {{"10"}, {"20"}, {"30"}});

    AvgAggregation agg("v");
    agg.RunBatch(batch, SingleGroup(3), 0);
    EXPECT_EQ(agg.GetResultValue(0), "20");
}

TEST(Aggregation, MaxNumeric) {
    Schema schema({"v"}, {Type::int64});
    auto batch = MakeBatch(schema, {{"5"}, {"42"}, {"7"}});

    MaxAggregation agg("v");
    agg.RunBatch(batch, SingleGroup(3), 0);
    EXPECT_EQ(agg.GetResultValue(0), "42");
}

TEST(Aggregation, MinNumeric) {
    Schema schema({"v"}, {Type::int64});
    auto batch = MakeBatch(schema, {{"5"}, {"42"}, {"7"}});

    MinAggregation agg("v");
    agg.RunBatch(batch, SingleGroup(3), 0);
    EXPECT_EQ(agg.GetResultValue(0), "5");
}

TEST(Aggregation, MaxString) {
    Schema schema({"s"}, {Type::str});
    auto batch = MakeBatch(schema, {{"apple"}, {"pear"}, {"banana"}});

    MaxAggregation agg("s");
    agg.RunBatch(batch, SingleGroup(3), 0);
    EXPECT_EQ(agg.GetResultValue(0), "pear");
    EXPECT_EQ(agg.GetResultType(), Type::str);
}

TEST(Aggregation, MinString) {
    Schema schema({"s"}, {Type::str});
    auto batch = MakeBatch(schema, {{"apple"}, {"pear"}, {"banana"}});

    MinAggregation agg("s");
    agg.RunBatch(batch, SingleGroup(3), 0);
    EXPECT_EQ(agg.GetResultValue(0), "apple");
}

TEST(Aggregation, MaxDate) {
    Schema schema({"d"}, {Type::date});
    auto batch = MakeBatch(schema, {{"2013-07-01"}, {"2013-08-15"}, {"2013-07-20"}});

    MaxAggregation agg("d");
    agg.RunBatch(batch, SingleGroup(3), 0);
    EXPECT_EQ(agg.GetResultValue(0), "2013-08-15");
    EXPECT_EQ(agg.GetResultType(), Type::date);
}

TEST(Aggregation, CountDistinct) {
    Schema schema({"v"}, {Type::int64});
    auto batch = MakeBatch(schema, {{"1"}, {"2"}, {"1"}, {"3"}, {"2"}});

    CountDistinctAggregation agg("v");
    agg.RunBatch(batch, SingleGroup(5), 0);
    EXPECT_EQ(agg.GetResultValue(0), "3");
}

TEST(Aggregation, CountDistinctString) {
    Schema schema({"s"}, {Type::str});
    auto batch = MakeBatch(schema, {{"a"}, {"b"}, {"a"}, {"a"}});

    CountDistinctAggregation agg("s");
    agg.RunBatch(batch, SingleGroup(4), 0);
    EXPECT_EQ(agg.GetResultValue(0), "2");
}

TEST(Aggregation, MultipleGroups) {
    Schema schema({"v"}, {Type::int64});
    auto batch = MakeBatch(schema, {{"10"}, {"100"}, {"20"}, {"200"}});

    std::vector<uint32_t> gi{0, 1, 0, 1};
    SumAggregation agg("v");
    agg.RunBatch(batch, gi, 1);
    EXPECT_EQ(agg.GetResultValue(0), "30");
    EXPECT_EQ(agg.GetResultValue(1), "300");
}

TEST(Aggregation, AccumulatesAcrossBatches) {
    Schema schema({"v"}, {Type::int64});
    auto batch1 = MakeBatch(schema, {{"1"}, {"2"}});
    auto batch2 = MakeBatch(schema, {{"3"}, {"4"}});

    SumAggregation agg("v");
    agg.RunBatch(batch1, SingleGroup(2), 0);
    agg.RunBatch(batch2, SingleGroup(2), 0);
    EXPECT_EQ(agg.GetResultValue(0), "10");
}

TEST(Aggregation, CloneIsIndependent) {
    SumAggregation agg("col", "myname");
    auto clone = agg.Clone();
    EXPECT_EQ(clone->result_name, "myname");
    EXPECT_EQ(clone->GetResultType(), Type::int128);
}
