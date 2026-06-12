#include <gtest/gtest.h>
#include <stdexcept>

#include "utils.h"

TEST(Utils, DateInt32RoundTrip) {
    Date date{.day = 14, .month = 7, .year = 2013};
    const int32_t packed = DateToInt32(date);
    const Date back = Int32ToDate(packed);
    EXPECT_EQ(back.day, 14);
    EXPECT_EQ(back.month, 7);
    EXPECT_EQ(back.year, 2013);
}

TEST(Utils, TimeStampInt64RoundTrip) {
    TimeStamp ts;
    ts.year = 2013;
    ts.month = 7;
    ts.day = 14;
    ts.h = 23;
    ts.m = 59;
    ts.s = 58;
    const int64_t packed = TimeStampToInt64(ts);
    const TimeStamp back = Int64ToTimeStamp(packed);
    EXPECT_EQ(back.year, 2013);
    EXPECT_EQ(back.month, 7);
    EXPECT_EQ(back.day, 14);
    EXPECT_EQ(back.h, 23);
    EXPECT_EQ(back.m, 59);
    EXPECT_EQ(back.s, 58);
}

TEST(Utils, DateStringRoundTrip) {
    const Date date = StringToDate("2013-07-01");
    EXPECT_EQ(date.year, 2013);
    EXPECT_EQ(date.month, 7);
    EXPECT_EQ(date.day, 1);
    EXPECT_EQ(DateToString(date), "2013-07-01");
}

TEST(Utils, DateStringRejectsBadFormat) {
    EXPECT_THROW(StringToDate("2013/07/01"), std::invalid_argument);
    EXPECT_THROW(StringToDate("2013-13-01"), std::invalid_argument);
    EXPECT_THROW(StringToDate("2013-07-40"), std::invalid_argument);
    EXPECT_THROW(StringToDate("short"), std::invalid_argument);
}

TEST(Utils, TimeStampStringRoundTrip) {
    const TimeStamp ts = StringToTimeStamp("2013-07-01 12:34:56");
    EXPECT_EQ(ts.year, 2013);
    EXPECT_EQ(ts.month, 7);
    EXPECT_EQ(ts.day, 1);
    EXPECT_EQ(ts.h, 12);
    EXPECT_EQ(ts.m, 34);
    EXPECT_EQ(ts.s, 56);
    EXPECT_EQ(TimeStampToString(ts), "2013-07-01 12:34:56");
}

TEST(Utils, TimeStampStringRejectsBadFormat) {
    EXPECT_THROW(StringToTimeStamp("2013-07-01"), std::invalid_argument);
    EXPECT_THROW(StringToTimeStamp("2013-07-01 25:00:00"), std::invalid_argument);
    EXPECT_THROW(StringToTimeStamp("2013-07-01 12:60:00"), std::invalid_argument);
}

TEST(Utils, TypeToString) {
    EXPECT_EQ(TypeToString(Type::int64), "int64");
    EXPECT_EQ(TypeToString(Type::int128), "int128");
    EXPECT_EQ(TypeToString(Type::str), "string");
    EXPECT_EQ(TypeToString(Type::double_), "double");
    EXPECT_EQ(TypeToString(Type::date), "DATE");
    EXPECT_EQ(TypeToString(Type::timestamp), "TIMESTAMP");
}
