#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>

namespace Constants {
    const size_t BATCH_SIZE = 100000;
    const size_t MAX_COLUMN_COUNT = UINT32_MAX;
    const size_t ORDER_BY_LIMIT = 1000;
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

std::string_view TypeToString(Type t);

namespace concepts {
    template<typename T>
    concept BinarySerializable = 
        std::is_integral_v<T> || 
        std::is_same_v<T, float> || 
        std::is_same_v<T, double>;
};
