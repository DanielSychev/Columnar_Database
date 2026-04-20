#include <gtest/gtest.h>
#include <memory>
#include <sstream>

#include "CsvMfWriter/writer.h"
#include "engine/data_storage/batch.h"
#include "engine/data_storage/schema.h"
#include "engine/serialization/batch_serialization.h"
#include "queries_executor/executor.h"
#include "queries_executor/operator.h"

namespace {
std::stringstream BuildMfStreamWithSingleBatch() {
    Schema schema({"id", "name", "score"}, {Type::int64, Type::str, Type::int64});
    Batch batch(schema, 10);
    batch.AddRow({"1", "Alice", "10"});
    batch.AddRow({"2", "Bob", "20"});

    std::stringstream mf_stream;
    Writer writer(mf_stream);

    const size_t batch_meta_position = batch_serialization::WriteMfBatch(batch, writer);
    const size_t footer_position = writer.TellPos();

    writer.BinaryWrite(schema.NumColums());
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
    auto filter = std::make_shared<FilterOperator>(scan, "score", std::string("20"));
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
