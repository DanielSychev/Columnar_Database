#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "CsvMfWriter/writer.h"
#include "engine/data_storage/batch.h"
#include "engine/data_storage/schema.h"
#include "engine/serialization/batch_serialization.h"
#include "queries_executor/aggregation.h"
#include "queries_executor/executor.h"
#include "queries_executor/operator.h"
#include "utils.h"

namespace {
std::stringstream BuildMfStream() {
    Schema schema({"id", "category", "value"}, {Type::int64, Type::str, Type::int64});
    Batch batch(schema, 10);
    batch.AddRow({"1", "a", "10"});
    batch.AddRow({"2", "b", "20"});
    batch.AddRow({"3", "a", "30"});
    batch.AddRow({"4", "b", "5"});

    std::stringstream mf_stream;
    Writer writer(mf_stream);
    const size_t batch_meta_position = batch_serialization::WriteMfBatch(batch, writer);
    const size_t footer_position = writer.TellPos();
    writer.BinaryWrite(schema.NumColumns());
    schema.PrintSchema(writer);
    writer.BinaryWrite(static_cast<size_t>(1));
    writer.BinaryWrite(batch_meta_position);
    writer.BinaryWrite(footer_position);
    return mf_stream;
}

std::vector<Row> CollectRows(std::shared_ptr<PipelineExecutor> executor) {
    std::vector<Row> all;
    while (auto batch = executor->NextBatch()) {
        for (size_t i = 0; i < batch->RowsCount(); ++i) {
            if (batch->HasMask() && batch->banned_rows[i]) {
                continue;
            }
            all.push_back(batch->GetRow(i));
        }
    }
    return all;
}
}

TEST(GroupByExecutor, SumAndCountPerGroup) {
    auto mf = BuildMfStream();
    auto scan = std::make_shared<ScanOperator>(mf, std::vector<std::string>{"category", "value"});
    std::vector<std::shared_ptr<Aggregation>> aggs{
        std::make_shared<SumAggregation>("value", "s"),
        std::make_shared<CountAggregation>("c"),
    };
    auto group_by = std::make_shared<GroupByOperator>(scan, std::vector<std::string>{"category"}, aggs);
    auto executor = ExecuteOperator(group_by);

    std::map<std::string, std::pair<std::string, std::string>> by_cat;
    for (const auto& row : CollectRows(executor)) {
        ASSERT_EQ(row.size(), 3u);
        by_cat[row[0]] = {row[1], row[2]};
    }
    ASSERT_EQ(by_cat.size(), 2u);
    EXPECT_EQ(by_cat["a"].first, "40");
    EXPECT_EQ(by_cat["a"].second, "2");
    EXPECT_EQ(by_cat["b"].first, "25");
    EXPECT_EQ(by_cat["b"].second, "2");
}

TEST(OrderByExecutor, DescendingWithLimit) {
    auto mf = BuildMfStream();
    auto scan = std::make_shared<ScanOperator>(mf, std::vector<std::string>{"id", "value"});
    auto order_by = std::make_shared<OrderByOperator>(
        scan, std::vector<std::string>{"value"}, /*descending=*/true, /*limit=*/2);
    auto executor = ExecuteOperator(order_by);

    auto rows = CollectRows(executor);
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0][1], "30");
    EXPECT_EQ(rows[1][1], "20");
}

TEST(OrderByExecutor, AscendingFullSort) {
    auto mf = BuildMfStream();
    auto scan = std::make_shared<ScanOperator>(mf, std::vector<std::string>{"id", "value"});
    auto order_by = std::make_shared<OrderByOperator>(
        scan, std::vector<std::string>{"value"}, /*descending=*/false);
    auto executor = ExecuteOperator(order_by);

    auto rows = CollectRows(executor);
    ASSERT_EQ(rows.size(), 4u);
    EXPECT_EQ(rows[0][1], "5");
    EXPECT_EQ(rows[1][1], "10");
    EXPECT_EQ(rows[2][1], "20");
    EXPECT_EQ(rows[3][1], "30");
}

TEST(OrderByExecutor, WithOffset) {
    auto mf = BuildMfStream();
    auto scan = std::make_shared<ScanOperator>(mf, std::vector<std::string>{"id", "value"});
    auto order_by = std::make_shared<OrderByOperator>(
        scan, std::vector<std::string>{"value"}, /*descending=*/false, /*limit=*/2, /*offset=*/1);
    auto executor = ExecuteOperator(order_by);

    auto rows = CollectRows(executor);
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0][1], "10");
    EXPECT_EQ(rows[1][1], "20");
}

TEST(LimitExecutor, TruncatesToLimit) {
    auto mf = BuildMfStream();
    auto scan = std::make_shared<ScanOperator>(mf, std::vector<std::string>{"id", "value"});
    auto limit = std::make_shared<LimitOperator>(scan, 2);
    auto executor = ExecuteOperator(limit);

    auto rows = CollectRows(executor);
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0][0], "1");
    EXPECT_EQ(rows[1][0], "2");
}

TEST(AggregateExecutor, GlobalSum) {
    auto mf = BuildMfStream();
    auto scan = std::make_shared<ScanOperator>(mf, std::vector<std::string>{"value"});
    std::vector<std::shared_ptr<Aggregation>> aggs{std::make_shared<SumAggregation>("value", "total")};
    auto aggregate = std::make_shared<AggregateOperator>(scan, aggs);
    auto executor = ExecuteOperator(aggregate);

    auto rows = CollectRows(executor);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0][0], "65");
}
