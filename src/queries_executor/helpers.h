#pragma once

#include "engine/data_storage/column.h"
#include "engine/data_storage/schema.h"
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace queries_executor_detail {
inline std::pair<Type, size_t> ResolveColumn(
    const Schema& schema,
    const std::string& column_name,
    std::string_view context
) {
    if (auto type_and_pos = schema.GetTypeAndPos(column_name); type_and_pos.has_value()) {
        return type_and_pos.value();
    }
    throw std::runtime_error(
        "no such column '" + column_name + "' in schema (in " + std::string(context) + ")"
    );
}

inline int8_t CompareTypedValues(
    Type type,
    std::string_view left,
    std::string_view right,
    std::string_view context
) {
    switch (type) {
        case Type::int128: {
            const auto a = column_detail::ParseInt128(left);
            const auto b = column_detail::ParseInt128(right);
            return (a < b ? -1 : (a > b ? 1 : 0));
        }
        case Type::int64:
        case Type::int32:
        case Type::int16:
        case Type::int8: {
            const auto a = std::stoll(std::string(left));
            const auto b = std::stoll(std::string(right));
            return (a < b ? -1 : (a > b ? 1 : 0));
        }
        case Type::double_: {
            const auto a = std::stod(std::string(left));
            const auto b = std::stod(std::string(right));
            return (a < b ? -1 : (a > b ? 1 : 0));
        }
        case Type::date:
        case Type::timestamp:
        case Type::str:
            return (left < right ? -1 : (left > right ? 1 : 0));
        default:
            throw std::runtime_error(
                "unsupported type for comparison (in " + std::string(context) + ")"
            );
    }
}
}
