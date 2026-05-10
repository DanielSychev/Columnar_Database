#include "queries_executor/transform.h"
#include <cstdint>
#include <regex>
#include <stdexcept>
#include <string_view>

namespace {
std::pair<Type, size_t> ResolveColumn(const Schema& schema, const std::string& column_name) {
    if (auto type_and_pos = schema.GetTypeAndPos(column_name); type_and_pos.has_value()) {
        return type_and_pos.value();
    }
    throw std::runtime_error("no such column in schema (in Transform)");
}

int64_t ParseMinute(std::string_view timestamp) {
    if (timestamp.size() < 16 || timestamp[13] != ':') {
        throw std::runtime_error("wrong timestamp format for ExtractMinuteTransform");
    }
    const char tens = timestamp[14];
    const char ones = timestamp[15];
    if (tens < '0' || tens > '5' || ones < '0' || ones > '9') {
        throw std::runtime_error("wrong minute format for ExtractMinuteTransform");
    }
    return static_cast<int64_t>((tens - '0') * 10 + (ones - '0'));
}
}

Transform::Transform(const std::string& result_name_) : result_name(result_name_) {
}


ExtractMinuteTransform::ExtractMinuteTransform(const std::string& source_column_name_, const std::string& result_name_)
    : Transform(result_name_), source_column_name(source_column_name_) {
    if (result_name_.empty()) {
        result_name = "extract(minute FROM " + source_column_name_ + ")";
    }
}

Type ExtractMinuteTransform::ResultType(const Schema& input_schema) const {
    const auto [input_type, column_index] = ResolveColumn(input_schema, source_column_name);
    (void)column_index; // чтобы не было предупреждения о неиспользуемой переменной
    if (input_type != Type::timestamp) {
        throw std::runtime_error("ExtractMinuteTransform expects TIMESTAMP column");
    }
    return Type::int64;
}

std::shared_ptr<Column> ExtractMinuteTransform::Apply(const Batch& batch) const {
    const auto [input_type, column_index] = ResolveColumn(batch.GetSchema(), source_column_name);
    if (input_type != Type::timestamp) {
        throw std::runtime_error("ExtractMinuteTransform expects TIMESTAMP column");
    }

    const auto* timestamp_column = dynamic_cast<const TimeStampColumn*>(&batch.ColumnAt(column_index));
    if (!timestamp_column) {
        throw std::runtime_error("wrong column implementation for TIMESTAMP in ExtractMinuteTransform");
    }

    std::vector<int64_t> minutes;
    minutes.reserve(batch.RowsCount());
    for (const auto& value : timestamp_column->Data()) {
        minutes.push_back(ParseMinute(value));
    }
    return std::make_shared<Int64Column>(minutes);
}


LengthTransform::LengthTransform(const std::string& source_column_name_, const std::string& result_name_)
    : Transform(result_name_), source_column_name(source_column_name_) {
    if (result_name_.empty()) {
        result_name = "length(" + source_column_name_ + ")";
    }
}

Type LengthTransform::ResultType(const Schema& input_schema) const {
    const auto [input_type, column_index] = ResolveColumn(input_schema, source_column_name);
    (void)column_index; // чтобы не было предупреждения о неиспользуемой переменной
    if (input_type != Type::str) {
        throw std::runtime_error("LengthTransform expects STRING column");
    }
    return Type::int64;
}

std::shared_ptr<Column> LengthTransform::Apply(const Batch& batch) const {
    const auto [input_type, column_index] = ResolveColumn(batch.GetSchema(), source_column_name);
    if (input_type != Type::str) {
        throw std::runtime_error("LengthTransform expects STRING column");
    }

    const auto* str_column = dynamic_cast<const StrColumn*>(&batch.ColumnAt(column_index));
    if (!str_column) {
        throw std::runtime_error("wrong column implementation for STRING in LengthTransform");
    }
    std::vector<int64_t> lengths;
    lengths.reserve(batch.RowsCount());
    for (const auto& value : str_column->Data()) {
        lengths.push_back(static_cast<int64_t>(value.size()));
    }
    return std::make_shared<Int64Column>(lengths);
}


RegexpReplaceTransform::RegexpReplaceTransform(const std::string& source_column_name_, const std::string& pattern_, const std::string& replacement_, const std::string& result_name_)
    : Transform(result_name_), source_column_name(source_column_name_), regex_pattern(pattern_), replacement(replacement_) {
    if (result_name_.empty()) {
        result_name = "regexp_replace(" + source_column_name_ + ", '" + pattern_ + "', '" + replacement_ + "')";
    }
}

Type RegexpReplaceTransform::ResultType(const Schema& input_schema) const {
    const auto [input_type, column_index] = ResolveColumn(input_schema, source_column_name);
    (void)column_index; // чтобы не было предупреждения о неиспользуемой переменной
    if (input_type != Type::str) {
        throw std::runtime_error("RegexpReplaceTransform expects STRING column");
    }
    return Type::str;
}

std::shared_ptr<Column> RegexpReplaceTransform::Apply(const Batch& batch) const {
    const auto [input_type, column_index] = ResolveColumn(batch.GetSchema(), source_column_name);
    if (input_type != Type::str) {
        throw std::runtime_error("RegexpReplaceTransform expects STRING column");
    }

    const auto* str_column = dynamic_cast<const StrColumn*>(&batch.ColumnAt(column_index));
    if (!str_column) {
        throw std::runtime_error("wrong column implementation for STRING in RegexpReplaceTransform");
    }
    std::vector<std::string> values;
    values.reserve(batch.RowsCount());
    for (const auto& value : str_column->Data()) {
        values.push_back(std::regex_replace(value, regex_pattern, replacement));
    }
    return std::make_shared<StrColumn>(values);
}
