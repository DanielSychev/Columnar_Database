#include <gtest/gtest.h>
#include <memory>
#include <sstream>

#include "CsvMfWriter/writer.h"
#include "engine/data_storage/batch.h"
#include "engine/data_storage/schema.h"
#include "engine/serialization/batch_serialization.h"
#include "queries_executor/executor.h"
#include "queries_executor/operator.h"
#include "queries_executor/transform.h"

namespace {
std::shared_ptr<FilterOperator> MakeFilter(
    std::shared_ptr<Operator> child_op,
    std::vector<std::string>&& column_names,
    std::vector<std::string>&& values,
    std::vector<CompareSign>&& signs
) {
    return std::make_shared<FilterOperator>(child_op, std::move(column_names), std::move(values), std::move(signs));
}

std::stringstream BuildMfStreamWithSingleBatch() {
    Schema schema({"id", "name", "score"}, {Type::int64, Type::str, Type::int64});
    Batch batch(schema, 10);
    batch.AddRow({"1", "Alice", "10"});
    batch.AddRow({"2", "Bob", "20"});

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

std::stringstream BuildMfStreamWithLikeBatch() {
    Schema schema({"id", "url"}, {Type::int64, Type::str});
    Batch batch(schema, 10);
    batch.AddRow({"1", "http://example.com/google%2F"});
    batch.AddRow({"2", "http://example.com/other"});

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

std::stringstream BuildMfStreamWithTimestampBatch() {
    Schema schema({"id", "event_time"}, {Type::int64, Type::timestamp});
    Batch batch(schema, 10);
    batch.AddRow({"1", "2013-07-15 09:28:29"});
    batch.AddRow({"2", "2013-07-15 13:51:09"});

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
}

TEST(ScanExecutor, ReadsRequestedColumnsByNameAndOrder) {
    std::stringstream mf_stream = BuildMfStreamWithSingleBatch();

    auto scan = std::make_shared<ScanOperator>(
        mf_stream,
        std::vector<std::string>{"score", "id"}
    );
    auto executor = ExecuteOperator(scan);

    auto batch = executor->NextBatch();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->RowsCount(), 2);
    EXPECT_EQ(batch->ColumnsCount(), 2);

    std::stringstream csv_output;
    Writer csv_writer(csv_output);
    batch_serialization::WriteCsvBatch(*batch, csv_writer);

    EXPECT_EQ(csv_output.str(), "10,1\n20,2\n");
    EXPECT_EQ(executor->NextBatch(), nullptr);
}

TEST(FilterExecutor, CopiesColumnsSkippingBannedRows) {
    std::stringstream mf_stream = BuildMfStreamWithSingleBatch();

    auto scan = std::make_shared<ScanOperator>(
        mf_stream,
        std::vector<std::string>{"id", "score"}
    );
    auto filter = MakeFilter(scan, {"score"}, {"20"}, {CompareSign::EQUAL});
    auto executor = ExecuteOperator(filter);

    auto batch = executor->NextBatch();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->RowsCount(), 1);
    EXPECT_EQ(batch->ColumnsCount(), 2);

    std::stringstream csv_output;
    Writer csv_writer(csv_output);
    batch_serialization::WriteCsvBatch(*batch, csv_writer);

    EXPECT_EQ(csv_output.str(), "2,20\n");
    EXPECT_EQ(executor->NextBatch(), nullptr);
}

TEST(FilterExecutor, SupportsSqlLikeWildcards) {
    std::stringstream mf_stream = BuildMfStreamWithSingleBatch();

    auto scan = std::make_shared<ScanOperator>(
        mf_stream,
        std::vector<std::string>{"id", "name"}
    );
    auto filter = MakeFilter(scan, {"name"}, {"A_i%e"}, {CompareSign::LIKE});
    auto executor = ExecuteOperator(filter);

    auto batch = executor->NextBatch();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->RowsCount(), 1);
    EXPECT_EQ(batch->ColumnsCount(), 2);

    std::stringstream csv_output;
    Writer csv_writer(csv_output);
    batch_serialization::WriteCsvBatch(*batch, csv_writer);

    EXPECT_EQ(csv_output.str(), "1,Alice\n");
    EXPECT_EQ(executor->NextBatch(), nullptr);
}

TEST(FilterExecutor, LikeMatchesValuesContainingPercentCharacters) {
    std::stringstream mf_stream = BuildMfStreamWithLikeBatch();

    auto scan = std::make_shared<ScanOperator>(
        mf_stream,
        std::vector<std::string>{"id", "url"}
    );
    auto filter = MakeFilter(scan, {"url"}, {"%google%"}, {CompareSign::LIKE});
    auto executor = ExecuteOperator(filter);

    auto batch = executor->NextBatch();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->RowsCount(), 1);
    EXPECT_EQ(batch->ColumnsCount(), 2);

    std::stringstream csv_output;
    Writer csv_writer(csv_output);
    batch_serialization::WriteCsvBatch(*batch, csv_writer);

    EXPECT_EQ(csv_output.str(), "1,http://example.com/google%2F\n");
    EXPECT_EQ(executor->NextBatch(), nullptr);
}

TEST(TransformExecutor, AppendsDerivedColumnsSequentially) {
    std::stringstream mf_stream = BuildMfStreamWithTimestampBatch();

    auto scan = std::make_shared<ScanOperator>(
        mf_stream,
        std::vector<std::string>{"id", "event_time"}
    );
    auto transforms = std::vector<std::shared_ptr<Transform>>{
        std::make_shared<ExtractMinuteTransform>("event_time", "minute")
    };
    auto transform = std::make_shared<TransformsOperator>(scan, std::move(transforms));
    auto executor = ExecuteOperator(transform);

    auto batch = executor->NextBatch();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->RowsCount(), 2);
    EXPECT_EQ(batch->ColumnsCount(), 3);

    std::stringstream csv_output;
    Writer csv_writer(csv_output);
    batch_serialization::WriteCsvBatch(*batch, csv_writer);

    EXPECT_EQ(csv_output.str(), "1,2013-07-15 09:28:29,28\n2,2013-07-15 13:51:09,51\n");
    EXPECT_EQ(executor->NextBatch(), nullptr);
}
