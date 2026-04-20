#include <gtest/gtest.h>
#include <sstream>

#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"
#include "engine/data_storage/batch.h"
#include "engine/data_storage/schema.h"
#include "engine/serialization/batch_serialization.h"

namespace {
Schema MakeTestSchema() {
    Schema schema;
    schema.names = {"id", "name"};
    schema.types = {Type::int64, Type::str};
    schema.column_count = schema.names.size();
    return schema;
}
}

TEST(BatchSerialization, CsvToMfToCsvRoundTrip) {
    Schema schema = MakeTestSchema();

    std::stringstream csv_input;
    csv_input << "1,Alice\n";
    csv_input << "2,\"Bob, Jr.\"\n";

    Reader csv_reader(csv_input);
    Batch csv_batch(schema, 10);
    ASSERT_TRUE(batch_serialization::ReadCsvBatch(csv_reader, csv_batch));
    ASSERT_EQ(csv_batch.RowsCount(), 2);

    std::stringstream mf_stream;
    Writer mf_writer(mf_stream);
    batch_serialization::WriteMfBatch(csv_batch, mf_writer);

    mf_stream.seekg(0);
    Reader mf_reader(mf_stream);
    Batch mf_batch(schema, 10);
    ASSERT_TRUE(batch_serialization::ReadMfBatch(mf_reader, mf_batch));
    ASSERT_EQ(mf_batch.RowsCount(), 2);

    std::stringstream csv_output;
    Writer csv_writer(csv_output);
    batch_serialization::WriteCsvBatch(mf_batch, csv_writer);

    EXPECT_EQ(csv_output.str(), "1,Alice\n2,\"Bob, Jr.\"\n");
}

TEST(BatchSerialization, CsvWriterUsesOnlyRealRowCount) {
    Schema schema = MakeTestSchema();

    std::stringstream csv_input("1,Alice\n");
    Reader csv_reader(csv_input);
    Batch batch(schema, 10);

    ASSERT_TRUE(batch_serialization::ReadCsvBatch(csv_reader, batch));
    ASSERT_EQ(batch.RowsCount(), 1);

    std::stringstream csv_output;
    Writer csv_writer(csv_output);
    batch_serialization::WriteCsvBatch(batch, csv_writer);

    EXPECT_EQ(csv_output.str(), "1,Alice\n");
}

TEST(BatchSerialization, CanWriteSchemaLessBatchBuiltFromColumns) {
    Batch batch(10);

    auto id_column = std::make_shared<Int64Column>();
    id_column->AddElem("1");
    id_column->AddElem("2");

    auto name_column = std::make_shared<StrColumn>();
    name_column->AddElem("Alice");
    name_column->AddElem("Bob");

    batch.AddColumn(id_column);
    batch.AddColumn(name_column);

    ASSERT_FALSE(batch.HasSchema());
    ASSERT_EQ(batch.RowsCount(), 2);
    ASSERT_EQ(batch.ColumnsCount(), 2);

    std::stringstream csv_output;
    Writer csv_writer(csv_output);
    batch_serialization::WriteCsvBatch(batch, csv_writer);

    EXPECT_EQ(csv_output.str(), "1,Alice\n2,Bob\n");
}
