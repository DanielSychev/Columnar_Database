#include <gtest/gtest.h>
#include <stdexcept>
#include "CsvMfReader/reader.h"

TEST(CSV_Reader, BasicQuotes) {
    std::stringstream s(R"("Hello","World")");
    Reader a(s);
    std::vector<std::string> res;
    ASSERT_TRUE(a.ReadLine(res));
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0], "Hello");
    EXPECT_EQ(res[1], "World");
}

TEST(CSV_Reader, EscapedQuotes) {
    std::stringstream s(R"("He said ""Hello""","She said ""Hi""")");
    Reader a(s);
    std::vector<std::string> res;
    ASSERT_TRUE(a.ReadLine(res));
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0], R"(He said "Hello")");
    EXPECT_EQ(res[1], R"(She said "Hi")");
}

TEST(CSV_Reader, Empty) {
    std::stringstream s("");
    Reader a(s);
    std::vector<std::string> res;
    ASSERT_FALSE(a.ReadLine(res));
}

TEST(CSV_Reader, NewLine) {
    std::stringstream s(R"("Line1\nLine2","Single line",Multi
Line)");
    Reader a(s);
    std::vector<std::string> res;
    ASSERT_TRUE(a.ReadLine(res));
    ASSERT_EQ(res.size(), 3);
    EXPECT_EQ(res[0], R"(Line1\nLine2)");
    EXPECT_EQ(res[1], "Single line");
    EXPECT_EQ(res[2], "Multi");
}

TEST(CSV_Reader, EmptyFields) {
    std::stringstream s(R"("","value","")");
    Reader a(s);
    std::vector<std::string> res;
    ASSERT_TRUE(a.ReadLine(res));
    ASSERT_EQ(res.size(), 3);
    EXPECT_EQ(res[0], "");
    EXPECT_EQ(res[1], "value");
    EXPECT_EQ(res[2], "");
}


TEST(CSV_Reader, QuotesAtStartAndEnd) {
    std::stringstream s(R"("""Start quote","End quote""","Mixed""quote""here")");
    Reader a(s);
    std::vector<std::string> res;
    ASSERT_TRUE(a.ReadLine(res));
    ASSERT_EQ(res.size(), 3);
    EXPECT_EQ(res[0], R"("Start quote)");
    EXPECT_EQ(res[1], R"(End quote")");
    EXPECT_EQ(res[2], R"(Mixed"quote"here)");
}

TEST(CSV_Reader, JohnFruscianteSong) {
    std::stringstream s(R"("the,""adventures"",of,""rain"" dance maggie")");
    Reader a(s);
    std::vector<std::string> res;
    ASSERT_TRUE(a.ReadLine(res));
    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], R"(the,"adventures",of,"rain" dance maggie)");
}

TEST(CSV_Reader, WrongQuote) {
    std::stringstream s(R"("bla-bla-bla)");
    Reader a(s);
    std::vector<std::string> res;
    ASSERT_THROW(a.ReadLine(res), std::runtime_error);
}

TEST(CSV_Reader, DifferentLines) {
    std::stringstream s(R"(a
b)");
    Reader a(s);
    std::vector<std::string> res;
    ASSERT_TRUE(a.ReadLine(res));
    EXPECT_EQ(res[0], "a");
    ASSERT_TRUE(a.ReadLine(res));
    ASSERT_EQ(res[0], "b");
}

TEST(CSV_Reader, BinaryReadInt64) {
    std::stringstream s;
    Reader a(s);
    int64_t number = 1904921021041903;
    s.write(reinterpret_cast<const char*>(&number), sizeof(int64_t));
    int64_t value;
    a.BinaryRead(value);
    EXPECT_EQ(number, value);
}

TEST(CSV_Reader, BinaryReadUInt32) {
    std::stringstream s;
    Reader a(s);
    uint32_t number = 1904921021;
    s.write(reinterpret_cast<const char*>(&number), sizeof(uint32_t));
    uint32_t value;
    a.BinaryRead(value);
    EXPECT_EQ(number, value);
}

TEST(CSV_Reader, BinaryReadVectorInt64) {
    std::stringstream s;
    Reader a(s);
    std::vector<int64_t> numbers = {1, -2, 300, 4000};
    size_t count = numbers.size();
    s.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
    s.write(reinterpret_cast<const char*>(numbers.data()), static_cast<std::streamsize>(numbers.size() * sizeof(int64_t)));

    std::vector<int64_t> values;
    a.BinaryReadVector(values);
    EXPECT_EQ(values, numbers);
}
