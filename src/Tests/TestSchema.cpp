#include <gtest/gtest.h>
#include <sstream>
#include <stdexcept>

#include "CsvMfReader/reader.h"
#include "engine/data_storage/schema.h"
#include "utils.h"

TEST(Schema, BasicAccessors) {
    Schema schema({"id", "name", "score"}, {Type::int64, Type::str, Type::int64});
    EXPECT_EQ(schema.NumColumns(), 3u);
    EXPECT_EQ(schema.ColumnNameAt(0), "id");
    EXPECT_EQ(schema.ColumnNameAt(1), "name");
    EXPECT_EQ(schema.ColumnTypeAt(0), Type::int64);
    EXPECT_EQ(schema.ColumnTypeAt(1), Type::str);
}

TEST(Schema, GetTypeAndPos) {
    Schema schema({"id", "name"}, {Type::int64, Type::str});
    auto found = schema.GetTypeAndPos("name");
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->first, Type::str);
    EXPECT_EQ(found->second, 1u);
    EXPECT_FALSE(schema.GetTypeAndPos("missing").has_value());
}

TEST(Schema, HasColumn) {
    Schema schema({"id"}, {Type::int64});
    EXPECT_TRUE(schema.HasColumn("id"));
    EXPECT_FALSE(schema.HasColumn("nope"));
}

TEST(Schema, AddColumnGrows) {
    Schema schema;
    EXPECT_EQ(schema.NumColumns(), 0u);
    schema.AddColumn("a", Type::int32);
    schema.AddColumn("b", Type::date);
    EXPECT_EQ(schema.NumColumns(), 2u);
    EXPECT_EQ(schema.ColumnTypeAt(1), Type::date);
    EXPECT_TRUE(schema.HasColumn("b"));
    EXPECT_EQ(schema.GetTypeAndPos("b")->second, 1u);
}

TEST(Schema, OutOfRangeThrows) {
    Schema schema({"id"}, {Type::int64});
    EXPECT_THROW(schema.ColumnNameAt(5), std::out_of_range);
    EXPECT_THROW(schema.ColumnTypeAt(5), std::out_of_range);
}

TEST(Schema, MismatchedSizesThrows) {
    EXPECT_THROW(Schema({"a", "b"}, {Type::int64}), std::runtime_error);
}

TEST(Schema, ValidateType) {
    EXPECT_EQ(Schema::ValidateType("int64"), Type::int64);
    EXPECT_EQ(Schema::ValidateType("string"), Type::str);
    EXPECT_EQ(Schema::ValidateType("DATE"), Type::date);
    EXPECT_EQ(Schema::ValidateType("TIMESTAMP"), Type::timestamp);
    EXPECT_THROW(Schema::ValidateType("nonsense"), std::runtime_error);
}

TEST(Schema, ReadSchemaFromStream) {
    std::stringstream stream;
    stream << "id,int64\n";
    stream << "name,string\n";
    stream << "ts,TIMESTAMP\n";
    Reader reader(stream);

    Schema schema;
    schema.ReadSchema(reader);
    EXPECT_EQ(schema.NumColumns(), 3u);
    EXPECT_EQ(schema.ColumnNameAt(0), "id");
    EXPECT_EQ(schema.ColumnTypeAt(0), Type::int64);
    EXPECT_EQ(schema.ColumnTypeAt(1), Type::str);
    EXPECT_EQ(schema.ColumnTypeAt(2), Type::timestamp);
}
