#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include "engine/data_storage/batch.h"
#include "engine/data_storage/schema.h"
#include "queries_executor/transform.h"
#include "utils.h"

namespace {
std::shared_ptr<Batch> MakeBatch(const Schema& schema, const std::vector<Row>& rows) {
    auto batch = std::make_shared<Batch>(schema, rows.size());
    for (const auto& row : rows) {
        batch->AddRow(Row(row));
    }
    return batch;
}
}

TEST(Transform, Length) {
    Schema schema({"s"}, {Type::str});
    auto batch = MakeBatch(schema, {{"alice"}, {"bo"}, {""}});

    LengthTransform t("s");
    EXPECT_EQ(t.ResultType(batch->GetSchema()), Type::int64);
    auto col = t.Apply(*batch);
    EXPECT_EQ(col->GetElemToString(0), "5");
    EXPECT_EQ(col->GetElemToString(1), "2");
    EXPECT_EQ(col->GetElemToString(2), "0");
}

TEST(Transform, Add) {
    Schema schema({"v"}, {Type::int64});
    auto batch = MakeBatch(schema, {{"1"}, {"2"}, {"3"}});

    AddTransform t("v", 100);
    EXPECT_EQ(t.ResultType(batch->GetSchema()), Type::int64);
    auto col = t.Apply(*batch);
    EXPECT_EQ(col->GetElemToString(0), "101");
    EXPECT_EQ(col->GetElemToString(2), "103");
}

TEST(Transform, Sub) {
    Schema schema({"v"}, {Type::int64});
    auto batch = MakeBatch(schema, {{"10"}, {"20"}});

    SubTransform t("v", 5);
    auto col = t.Apply(*batch);
    EXPECT_EQ(col->GetElemToString(0), "5");
    EXPECT_EQ(col->GetElemToString(1), "15");
}

TEST(Transform, ConstantInt8) {
    Schema schema({"v"}, {Type::int64});
    auto batch = MakeBatch(schema, {{"1"}, {"2"}, {"3"}});

    ConstantInt8Transform t(7);
    EXPECT_EQ(t.ResultType(batch->GetSchema()), Type::int8);
    auto col = t.Apply(*batch);
    EXPECT_EQ(col->Size(), 3u);
    EXPECT_EQ(col->GetElemToString(0), "7");
    EXPECT_EQ(col->GetElemToString(2), "7");
}

TEST(Transform, Rename) {
    Schema schema({"v"}, {Type::int64});
    auto batch = MakeBatch(schema, {{"7"}, {"8"}});

    RenameTransform t("v");
    EXPECT_EQ(t.ResultType(batch->GetSchema()), Type::int64);
    auto col = t.Apply(*batch);
    EXPECT_EQ(col->GetElemToString(0), "7");
    EXPECT_EQ(col->GetElemToString(1), "8");
}

TEST(Transform, ExtractMinute) {
    Schema schema({"ts"}, {Type::timestamp});
    auto batch = MakeBatch(schema, {{"2013-07-01 10:34:56"}, {"2013-07-01 23:05:00"}});

    ExtractMinuteTransform t("ts");
    EXPECT_EQ(t.ResultType(batch->GetSchema()), Type::int64);
    auto col = t.Apply(*batch);
    EXPECT_EQ(col->GetElemToString(0), "34");
    EXPECT_EQ(col->GetElemToString(1), "5");
}

TEST(Transform, DateTruncMinute) {
    Schema schema({"ts"}, {Type::timestamp});
    auto batch = MakeBatch(schema, {{"2013-07-01 10:34:56"}});

    DateTruncMinuteTransform t("ts");
    EXPECT_EQ(t.ResultType(batch->GetSchema()), Type::timestamp);
    auto col = t.Apply(*batch);
    EXPECT_EQ(col->GetElemToString(0), "2013-07-01 10:34:00");
}

TEST(Transform, RegexpReplace) {
    Schema schema({"s"}, {Type::str});
    auto batch = MakeBatch(schema, {{"banana"}, {"kiwi"}});

    RegexpReplaceTransform t("s", "a", "X");
    EXPECT_EQ(t.ResultType(batch->GetSchema()), Type::str);
    auto col = t.Apply(*batch);
    EXPECT_EQ(col->GetElemToString(0), "bXnXnX");
    EXPECT_EQ(col->GetElemToString(1), "kiwi");
}

TEST(Transform, CaseWhen) {
    Schema schema({"flag", "name"}, {Type::int64, Type::str});
    auto batch = MakeBatch(schema, {{"1", "yes"}, {"0", "no"}, {"1", "also"}});

    CaseWhenTransform t(
        std::vector<std::string>{"flag"},
        std::vector<std::string>{"1"},
        std::vector<CompareSign>{CompareSign::EQUAL},
        "name",
        "",
        "result"
    );
    EXPECT_EQ(t.ResultType(batch->GetSchema()), Type::str);
    auto col = t.Apply(*batch);
    EXPECT_EQ(col->GetElemToString(0), "yes");
    EXPECT_EQ(col->GetElemToString(1), "");
    EXPECT_EQ(col->GetElemToString(2), "also");
}

TEST(Transform, AddRejectsNonInteger) {
    Schema schema({"s"}, {Type::str});
    auto batch = MakeBatch(schema, {{"x"}});

    AddTransform t("s", 1);
    EXPECT_THROW(t.ResultType(batch->GetSchema()), std::runtime_error);
}
