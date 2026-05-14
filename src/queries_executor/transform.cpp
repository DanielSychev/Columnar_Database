#include "queries_executor/transform.h"
#include "queries_executor/helpers.h"
#include <cstdint>
#include <regex>
#include <stdexcept>
#include <string_view>

namespace {
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

size_t ExpectSourceType(
    const Schema& schema,
    const std::string& source_column_name,
    Type expected_type,
    std::string_view expected_type_name,
    std::string_view context
) {
    const auto [input_type, column_index] =
        queries_executor_detail::ResolveColumn(schema, source_column_name, context);
    if (input_type != expected_type) {
        throw std::runtime_error(
            std::string(context) + " expects " + std::string(expected_type_name) + " column"
        );
    }
    return column_index;
}

template <typename ColumnT>
const ColumnT& GetTypedColumn(
    const Batch& batch,
    size_t column_index,
    std::string_view expected_type_name,
    std::string_view context
) {
    const auto* typed_column = dynamic_cast<const ColumnT*>(&batch.ColumnAt(column_index));
    if (!typed_column) {
        throw std::runtime_error(
            "wrong column implementation for " + std::string(expected_type_name) + " in " + std::string(context)
        );
    }
    return *typed_column;
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
    ExpectSourceType(
        input_schema,
        source_column_name,
        Type::timestamp,
        "TIMESTAMP",
        "ExtractMinuteTransform"
    );
    return Type::int64;
}

std::shared_ptr<Column> ExtractMinuteTransform::Apply(const Batch& batch) const {
    const size_t column_index = ExpectSourceType(
        batch.GetSchema(),
        source_column_name,
        Type::timestamp,
        "TIMESTAMP",
        "ExtractMinuteTransform"
    );
    const auto& timestamp_column =
        GetTypedColumn<TimeStampColumn>(batch, column_index, "TIMESTAMP", "ExtractMinuteTransform");

    std::vector<int64_t> minutes;
    minutes.reserve(batch.RowsCount());
    for (const auto& value : timestamp_column.Data()) {
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
    ExpectSourceType(
        input_schema,
        source_column_name,
        Type::str,
        "STRING",
        "LengthTransform"
    );
    return Type::int64;
}

std::shared_ptr<Column> LengthTransform::Apply(const Batch& batch) const {
    const size_t column_index = ExpectSourceType(
        batch.GetSchema(),
        source_column_name,
        Type::str,
        "STRING",
        "LengthTransform"
    );
    const auto& str_column = GetTypedColumn<StrColumn>(batch, column_index, "STRING", "LengthTransform");
    std::vector<int64_t> lengths;
    lengths.reserve(batch.RowsCount());
    for (const auto& value : str_column.Data()) {
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
    ExpectSourceType(
        input_schema,
        source_column_name,
        Type::str,
        "STRING",
        "RegexpReplaceTransform"
    );
    return Type::str;
}

std::shared_ptr<Column> RegexpReplaceTransform::Apply(const Batch& batch) const {
    const size_t column_index = ExpectSourceType(
        batch.GetSchema(),
        source_column_name,
        Type::str,
        "STRING",
        "RegexpReplaceTransform"
    );
    const auto& str_column =
        GetTypedColumn<StrColumn>(batch, column_index, "STRING", "RegexpReplaceTransform");
    std::vector<std::string> values;
    values.reserve(batch.RowsCount());
    for (const auto& value : str_column.Data()) {
        values.push_back(std::regex_replace(value, regex_pattern, replacement));
    }
    return std::make_shared<StrColumn>(values);
}
