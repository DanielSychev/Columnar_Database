#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include <vector>

#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"
#include "engine/data_storage/column.h"
#include "utils.h"

TEST(NumericColumn, ConstructFromStringsAndAccess) {
    Int64Column col(std::vector<std::string>{"10", "20", "30"});
    EXPECT_EQ(col.Size(), 3u);
    EXPECT_EQ(col.ValueAt(1), 20);
    EXPECT_EQ(col.GetElemToString(2), "30");
    EXPECT_EQ(col.Data().size(), 3u);
}

TEST(NumericColumn, AppendRawAndStr) {
    Int64Column col;
    col.AppendRaw(5);
    col.AppendStr("7");
    EXPECT_EQ(col.Size(), 2u);
    EXPECT_EQ(col.ValueAt(0), 5);
    EXPECT_EQ(col.ValueAt(1), 7);
}

TEST(NumericColumn, GetElemToStringOutOfRange) {
    Int64Column col(std::vector<std::string>{"1"});
    EXPECT_THROW(col.GetElemToString(5), std::out_of_range);
}

TEST(NumericColumn, FilterEqual) {
    Int64Column col(std::vector<std::string>{"10", "20", "30", "20"});
    std::vector<bool> banned(4, false);
    col.Filter("20", CompareSign::EQUAL, banned);
    EXPECT_TRUE(banned[0]);
    EXPECT_FALSE(banned[1]);
    EXPECT_TRUE(banned[2]);
    EXPECT_FALSE(banned[3]);
}

TEST(NumericColumn, FilterLess) {
    Int64Column col(std::vector<std::string>{"1", "5", "10"});
    std::vector<bool> banned(3, false);
    col.Filter("5", CompareSign::LESS, banned);
    EXPECT_FALSE(banned[0]);
    EXPECT_TRUE(banned[1]);
    EXPECT_TRUE(banned[2]);
}

TEST(NumericColumn, FilterIn) {
    Int64Column col(std::vector<std::string>{"1", "6", "3", "-1"});
    std::vector<bool> banned(4, false);
    col.Filter("-1,6", CompareSign::IN, banned);
    EXPECT_TRUE(banned[0]);
    EXPECT_FALSE(banned[1]);
    EXPECT_TRUE(banned[2]);
    EXPECT_FALSE(banned[3]);
}

TEST(NumericColumn, FilterLikeThrows) {
    Int64Column col(std::vector<std::string>{"1"});
    std::vector<bool> banned(1, false);
    EXPECT_THROW(col.Filter("x", CompareSign::LIKE, banned), std::invalid_argument);
}

TEST(NumericColumn, CompareAt) {
    Int64Column a(std::vector<std::string>{"10", "20"});
    Int64Column b(std::vector<std::string>{"20", "20"});
    EXPECT_LT(a.CompareAt(0, b, 0), 0);
    EXPECT_EQ(a.CompareAt(1, b, 1), 0);
    EXPECT_GT(b.CompareAt(0, a, 0), 0);
}

TEST(NumericColumn, AppendFrom) {
    Int64Column src(std::vector<std::string>{"42", "43"});
    Int64Column dst;
    dst.AppendFrom(src, 1);
    EXPECT_EQ(dst.Size(), 1u);
    EXPECT_EQ(dst.ValueAt(0), 43);
}

TEST(NumericColumn, CopyFiltered) {
    Int64Column col(std::vector<std::string>{"1", "2", "3"});
    std::vector<bool> banned{false, true, false};
    auto filtered = col.CopyFiltered(banned);
    EXPECT_EQ(filtered->Size(), 2u);
    EXPECT_EQ(filtered->GetElemToString(0), "1");
    EXPECT_EQ(filtered->GetElemToString(1), "3");
}

TEST(NumericColumn, CopyReordered) {
    Int64Column col(std::vector<std::string>{"10", "20", "30"});
    auto reordered = col.CopyReordered(std::vector<size_t>{2, 0, 1});
    EXPECT_EQ(reordered->GetElemToString(0), "30");
    EXPECT_EQ(reordered->GetElemToString(1), "10");
    EXPECT_EQ(reordered->GetElemToString(2), "20");
}

TEST(NumericColumn, BinaryWriteReadBufRoundTrip) {
    Int64Column col(std::vector<std::string>{"123", "456"});
    std::vector<char> buf;
    col.BinaryWriteInBuf(buf, 0);
    col.BinaryWriteInBuf(buf, 1);

    Int64Column restored;
    const char* ptr = buf.data();
    restored.BinaryReadFromBuf(ptr);
    restored.BinaryReadFromBuf(ptr);
    EXPECT_EQ(restored.ValueAt(0), 123);
    EXPECT_EQ(restored.ValueAt(1), 456);
}

TEST(NumericColumn, MfRoundTrip) {
    Int64Column col(std::vector<std::string>{"100", "105", "102", "100"});
    std::stringstream stream;
    Writer writer(stream);
    col.PrintMf(writer);

    Reader reader(stream);
    Int64Column restored;
    restored.ReadMf(reader);
    ASSERT_EQ(restored.Size(), 4u);
    EXPECT_EQ(restored.ValueAt(0), 100);
    EXPECT_EQ(restored.ValueAt(1), 105);
    EXPECT_EQ(restored.ValueAt(2), 102);
    EXPECT_EQ(restored.ValueAt(3), 100);
}

TEST(StrColumn, AppendAndAccess) {
    StrColumn col;
    col.AppendStr("alice");
    col.AppendStr("bob");
    EXPECT_EQ(col.Size(), 2u);
    EXPECT_EQ(col.GetElemView(0), "alice");
    EXPECT_EQ(col.GetElemToString(1), "bob");
}

TEST(StrColumn, ConstructFromVector) {
    StrColumn col(std::vector<std::string>{"x", "yy", "zzz"});
    EXPECT_EQ(col.Size(), 3u);
    EXPECT_EQ(col.GetElemView(2), "zzz");
}

TEST(StrColumn, FilterLike) {
    StrColumn col(std::vector<std::string>{"Alice", "Bob", "Alex"});
    std::vector<bool> banned(3, false);
    col.Filter("Al%", CompareSign::LIKE, banned);
    EXPECT_FALSE(banned[0]);
    EXPECT_TRUE(banned[1]);
    EXPECT_FALSE(banned[2]);
}

TEST(StrColumn, FilterNotEqualEmpty) {
    StrColumn col(std::vector<std::string>{"", "hello", ""});
    std::vector<bool> banned(3, false);
    col.Filter("", CompareSign::NOT_EQUAL, banned);
    EXPECT_TRUE(banned[0]);
    EXPECT_FALSE(banned[1]);
    EXPECT_TRUE(banned[2]);
}

TEST(StrColumn, CompareAt) {
    StrColumn a(std::vector<std::string>{"apple", "pear"});
    StrColumn b(std::vector<std::string>{"banana", "pear"});
    EXPECT_LT(a.CompareAt(0, b, 0), 0);
    EXPECT_EQ(a.CompareAt(1, b, 1), 0);
}

TEST(StrColumn, AppendFromAndReorder) {
    StrColumn src(std::vector<std::string>{"one", "two", "three"});
    StrColumn dst;
    dst.AppendFrom(src, 2);
    EXPECT_EQ(dst.GetElemView(0), "three");

    auto reordered = src.CopyReordered(std::vector<size_t>{1, 0});
    EXPECT_EQ(reordered->GetElemToString(0), "two");
    EXPECT_EQ(reordered->GetElemToString(1), "one");
}

TEST(StrColumn, MfRoundTrip) {
    StrColumn col(std::vector<std::string>{"red", "green", "red", "blue", "green"});
    std::stringstream stream;
    Writer writer(stream);
    col.PrintMf(writer);

    Reader reader(stream);
    StrColumn restored;
    restored.ReadMf(reader);
    ASSERT_EQ(restored.Size(), 5u);
    EXPECT_EQ(restored.GetElemView(0), "red");
    EXPECT_EQ(restored.GetElemView(1), "green");
    EXPECT_EQ(restored.GetElemView(3), "blue");
    EXPECT_EQ(restored.GetElemView(4), "green");
}

TEST(DateColumn, FromStringsAndFilter) {
    DateColumn col(std::vector<std::string>{"2013-07-01", "2013-07-15", "2013-08-01"});
    EXPECT_EQ(col.GetElemToString(0), "2013-07-01");

    std::vector<bool> banned(3, false);
    col.Filter("2013-07-15", CompareSign::LESS_OR_EQUAL, banned);
    EXPECT_FALSE(banned[0]);
    EXPECT_FALSE(banned[1]);
    EXPECT_TRUE(banned[2]);
}

TEST(DateColumn, MfRoundTrip) {
    DateColumn col(std::vector<std::string>{"2013-07-01", "2013-07-31"});
    std::stringstream stream;
    Writer writer(stream);
    col.PrintMf(writer);

    Reader reader(stream);
    DateColumn restored;
    restored.ReadMf(reader);
    ASSERT_EQ(restored.Size(), 2u);
    EXPECT_EQ(restored.GetElemToString(0), "2013-07-01");
    EXPECT_EQ(restored.GetElemToString(1), "2013-07-31");
}

TEST(TimeStampColumn, FromStringsAndMfRoundTrip) {
    TimeStampColumn col(std::vector<std::string>{"2013-07-01 10:00:00", "2013-07-01 10:01:30"});
    EXPECT_EQ(col.GetElemToString(1), "2013-07-01 10:01:30");

    std::stringstream stream;
    Writer writer(stream);
    col.PrintMf(writer);

    Reader reader(stream);
    TimeStampColumn restored;
    restored.ReadMf(reader);
    ASSERT_EQ(restored.Size(), 2u);
    EXPECT_EQ(restored.GetElemToString(0), "2013-07-01 10:00:00");
    EXPECT_EQ(restored.GetElemToString(1), "2013-07-01 10:01:30");
}

TEST(CreateColumn, ProducesCorrectTypes) {
    auto int_col = CreateColumn(Type::int64);
    int_col->AppendStr("7");
    EXPECT_EQ(int_col->GetElemToString(0), "7");

    auto str_col = CreateColumn(Type::str, std::vector<std::string>{"a", "b"});
    EXPECT_EQ(str_col->Size(), 2u);
    EXPECT_EQ(str_col->GetElemToString(1), "b");
}
