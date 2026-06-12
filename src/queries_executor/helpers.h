#pragma once

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
}
