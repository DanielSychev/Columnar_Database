#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <type_traits>

namespace Constants {
    const size_t BATCH_SIZE = 2<<14; // 16K rows
    const size_t MAX_COLUMN_COUNT = UINT32_MAX;
    const size_t ORDER_BY_NO_LIMIT = SIZE_MAX;
}

enum class CompareSign {
    EQUAL,
    NOT_EQUAL,
    LESS,
    GREATER,
    LESS_OR_EQUAL,
    GREATER_OR_EQUAL,
    LIKE,
    NOT_LIKE,
    IN
};

enum class Type {
    int128, int64, int32, int16, int8, double_, str, date, timestamp
};


struct Date {
    int8_t day;
    int8_t month;
    int16_t year;
};

struct TimeStamp : Date {
    int8_t h, m, s;
};

std::string_view TypeToString(Type t);
std::string DateToString(const Date& date);
Date StringToDate(const std::string& s);
std::string TimeStampToString(const TimeStamp& timestamp);
TimeStamp StringToTimeStamp(const std::string& s);

inline int32_t DateToInt32(const Date& date) {
    int32_t result;
    memcpy(&result, &date, 4);
    return result;
}

inline Date Int32ToDate(int32_t x) {
    Date date;
    memcpy(&date, &x, 4);
    return date;
}

inline int64_t TimeStampToInt64(const TimeStamp& ts) {
    return static_cast<int64_t>(ts.s)
         | (static_cast<int64_t>(ts.m)     << 8)
         | (static_cast<int64_t>(ts.h)     << 16)
         | (static_cast<int64_t>(ts.day)   << 24)
         | (static_cast<int64_t>(ts.month) << 32)
         | (static_cast<int64_t>(ts.year)  << 40);
}

inline TimeStamp Int64ToTimeStamp(int64_t x) {
    TimeStamp ts;
    ts.s     = static_cast<int8_t>(x & 0xFF);
    ts.m     = static_cast<int8_t>((x >> 8)  & 0xFF);
    ts.h     = static_cast<int8_t>((x >> 16) & 0xFF);
    ts.day   = static_cast<int8_t>((x >> 24) & 0xFF);
    ts.month = static_cast<int8_t>((x >> 32) & 0xFF);
    ts.year  = static_cast<int16_t>((x >> 40) & 0xFFFF);
    return ts;
}

namespace concepts {
    template<typename T>
    concept BinarySerializable =
        std::is_integral_v<T> ||
        std::is_enum_v<T>     ||
        std::is_same_v<T, float> ||
        std::is_same_v<T, double> ||
        std::is_same_v<T, Date> ||
        std::is_same_v<T, TimeStamp>;
};

enum class Tag : uint8_t {
    RAW = 0,
    BIT_PACKED = 1,
    DIC_ENCODED = 2,
};