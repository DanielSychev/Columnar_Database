#include <gtest/gtest.h>
#include "CsvMfWriter/writer.h"

TEST(MF_Writer, WriteElemInt) {
    std::stringstream s;
    Writer w(s);
    w.WriteElem(1, false);
    w.WriteElem(2, false);
    w.WriteElem(3, true);
    EXPECT_EQ(s.str(), "1,2,3\n");
}

TEST(MF_Writer, WriteElemBasic) {
    std::stringstream s;
    Writer w(s);
    w.WriteElem("Hello", false);
    w.WriteElem("World", true);
    EXPECT_EQ(s.str(), "Hello,World\n");
}

TEST(MF_Writer, WriteElemQuotes) {
    std::stringstream s;
    Writer w(s);
    w.WriteElem(R"(He said "Hello")", false);
    w.WriteElem(R"(She said "Hi")", true);
    EXPECT_EQ(s.str(), R"("He said ""Hello""","She said ""Hi""")""\n");
}

TEST(MF_Writer, WriteElemDelimetr) {
    std::stringstream s;
    Writer w(s);
    w.WriteElem("Hello, World!", true);
    EXPECT_EQ(s.str(), R"("Hello, World!")""\n");
}

TEST(MF_Writer, WriteElemHardTest) {
    std::stringstream s;
    Writer w(s);
    w.WriteElem(R"("Start quote)", false);
    w.WriteElem(R"(End quote")", false);
    w.WriteElem(R"(Mixed"quote"here)", true);
    EXPECT_EQ(s.str(), R"("""Start quote","End quote""","Mixed""quote""here")""\n");
}

TEST(MF_Writer, BinaryWriteInt64) {
    std::stringstream s;
    Writer w(s);
    int64_t number = 1904921021041903;
    w.BinaryWrite(number);
    int64_t value;
    s.read(reinterpret_cast<char*>(&value), sizeof(int64_t));
    EXPECT_EQ(number, value);
}

TEST(MF_Writer, BinaryWriteUInt32) {
    std::stringstream s;
    Writer w(s);
    uint32_t number = 1904921021;
    w.BinaryWrite(number);
    uint32_t value;
    s.read(reinterpret_cast<char*>(&value), sizeof(uint32_t));
    EXPECT_EQ(number, value);
}

TEST(MF_Writer, WriteElemWithNewLine) {
    std::stringstream s;
    Writer w(s);
    w.WriteElem("Line1\nLine2", true);
    EXPECT_EQ(s.str(), "\"Line1\nLine2\"\n");
}
