#include "queries_executor/transform.h"
#include "queries_executor/helpers.h"
#include <utils.h>
#include <cstdint>
#include <numeric>
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

std::string TruncateTimestampToMinute(std::string_view timestamp) {
    if (timestamp.size() < 16 || timestamp[4] != '-' || timestamp[7] != '-' ||
        timestamp[10] != ' ' || timestamp[13] != ':') {
        throw std::runtime_error("wrong timestamp format for DateTruncMinuteTransform");
    }

    std::string truncated(timestamp.substr(0, 16));
    truncated += ":00";
    return truncated;
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

// Type GetSourceType(
//     const Schema& schema,
//     const std::string& source_column_name
// ) {
//     const auto [input_type, column_index] =
//         queries_executor_detail::ResolveColumn(schema, source_column_name, "CheckSourceType");
//     return input_type;
// }


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

template <typename ColumnT>
std::vector<int64_t> ConvertToInt64(const Batch& batch, size_t column_index) {
    const auto& int_column = GetTypedColumn<ColumnT>(batch, column_index, "integer", "AddTransform");
    return std::vector<int64_t>(int_column.Data().begin(), int_column.Data().end());
}
}

Transform::Transform(const std::string& result_name_) : result_name(result_name_) {
}

const std::string& Transform::GetResultName() const {
    return result_name;
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
        GetTypedColumn<StrColumn>(batch, column_index, "TIMESTAMP", "ExtractMinuteTransform");

    std::vector<int64_t> minutes;
    minutes.reserve(batch.RowsCount());
    for (const auto& value : timestamp_column.Data()) {
        minutes.push_back(ParseMinute(value));
    }
    return std::make_shared<Int64Column>(minutes);
}

DateTruncMinuteTransform::DateTruncMinuteTransform(const std::string& source_column_name_, const std::string& result_name_)
    : Transform(result_name_), source_column_name(source_column_name_) {
    if (result_name_.empty()) {
        result_name = "DATE_TRUNC('minute', " + source_column_name_ + ")";
    }
}

Type DateTruncMinuteTransform::ResultType(const Schema& input_schema) const {
    ExpectSourceType(
        input_schema,
        source_column_name,
        Type::timestamp,
        "TIMESTAMP",
        "DateTruncMinuteTransform"
    );
    return Type::timestamp;
}

std::shared_ptr<Column> DateTruncMinuteTransform::Apply(const Batch& batch) const {
    const size_t column_index = ExpectSourceType(
        batch.GetSchema(),
        source_column_name,
        Type::timestamp,
        "TIMESTAMP",
        "DateTruncMinuteTransform"
    );
    const auto& timestamp_column =
        GetTypedColumn<StrColumn>(batch, column_index, "TIMESTAMP", "DateTruncMinuteTransform");

    std::vector<std::string> values;
    values.reserve(batch.RowsCount());
    for (const auto& value : timestamp_column.Data()) {
        values.push_back(TruncateTimestampToMinute(value));
    }
    return std::make_shared<TimeStampColumn>(std::move(values));
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


AddTransform::AddTransform(const std::string& source_column_name_, int64_t value_, const std::string& result_name_): Transform(result_name_), source_column_name(source_column_name_), value(value_) {
    if (result_name_.empty()) {
        result_name = source_column_name_ + " + " + std::to_string(value_);
    }
}

Type AddTransform::ResultType(const Schema& input_schema) const {
    const auto input_type = queries_executor_detail::ResolveColumn(input_schema, source_column_name, "AddTransform").first;
    if (allowed_types.find(input_type) == allowed_types.end()) {
        throw std::runtime_error(
            "AddTransform expects integer column as source, got " + std::string(TypeToString(input_type)) + " for column " + source_column_name
        );
    }
    return Type::int64;
}

std::shared_ptr<Column>  AddTransform::Apply(const Batch& batch) const {
    ResultType(batch.GetSchema()); // Check source type and throw if it's wrong.
    auto [input_type, column_index] = queries_executor_detail::ResolveColumn(batch.GetSchema(), source_column_name, "AddTransform");
    std::vector<int64_t> input_values;
    switch (input_type) {
        case Type::int64:
            input_values = ConvertToInt64<Int64Column>(batch, column_index);
             break;
        case Type::int32:
            input_values = ConvertToInt64<Int32Column>(batch, column_index);
            break;
        case Type::int16:
            input_values = ConvertToInt64<Int16Column>(batch, column_index);
            break;
        case Type::int8:
            input_values = ConvertToInt64<Int8Column>(batch, column_index);
            break;
        default:
            throw std::runtime_error("unexpected type for AddTransform source column");
    }
    for (auto& v : input_values) {
        v += value;
    }
    return std::make_shared<Int64Column>(input_values);
}

SubTransform::SubTransform(const std::string& source_column_name_, int64_t value_, const std::string& result_name_) : AddTransform(source_column_name_, -value_, result_name_) {
    if (result_name_.empty()) {
        result_name = source_column_name_ + " - " + std::to_string(value_);
    }
}


ConstantTransform::ConstantTransform(const std::string& value_, const std::string& result_name_) : Transform(result_name_), value(value_) {
    if (result_name_.empty()) {
        result_name = value_;
    }
}

Type ConstantTransform::ResultType(const Schema&) const {
    return Type::str;
}

std::shared_ptr<Column> ConstantTransform::Apply(const Batch& batch) const {
    std::vector<std::string> values(batch.RowsCount(), value);
    return std::make_shared<StrColumn>(values);
}


RenameTransform::RenameTransform(const std::string& source_column_name_, const std::string& result_name_)
    : Transform(result_name_), source_column_name(source_column_name_) {
    if (result_name_.empty()) {
        result_name = source_column_name_;
    }
}

Type RenameTransform::ResultType(const Schema& input_schema) const {
    return queries_executor_detail::ResolveColumn(input_schema, source_column_name, "RenameTransform").first;
}

std::shared_ptr<Column> RenameTransform::Apply(const Batch& batch) const {
    const auto [input_type, column_index] =
        queries_executor_detail::ResolveColumn(batch.GetSchema(), source_column_name, "RenameTransform");
    (void)input_type;

    std::vector<size_t> ordered(batch.RowsCount());
    std::iota(ordered.begin(), ordered.end(), 0);
    return batch.ColumnAt(column_index).CopyReordered(ordered);
}


CaseWhenTransform::CaseWhenTransform(const std::vector<std::string>& condition_column_names_, const std::vector<std::string>& condition_values_, const std::vector<CompareSign>& condition_signs_, 
    const std::string& column_true_, const std::string& column_false_, const std::string& result_name_) 
        : Transform(result_name_), condition_column_names(condition_column_names_), condition_values(condition_values_), condition_signs(condition_signs_), column_true(column_true_), column_false(column_false_) {
    
    if (condition_column_names.size() != condition_values.size() || condition_column_names.size() != condition_signs.size()) {
        throw std::runtime_error("condition vectors must have the same size for CaseWhenTransform");
    }
    if (result_name_.empty()) {
        result_name = "...";
    }
}

Type CaseWhenTransform::ResultType(const Schema& input_schema) const {
    if (input_schema.HasColumn(column_true)) {
        return queries_executor_detail::ResolveColumn(input_schema, column_true, "CaseWhenTransform").first;
    } else if (input_schema.HasColumn(column_false)) {
        return queries_executor_detail::ResolveColumn(input_schema, column_false, "CaseWhenTransform").first;
    } else {
        throw std::runtime_error("CaseWhenTransform: neither column_true nor column_false is present in input schema");
    }
}

std::shared_ptr<Column> CaseWhenTransform::Apply(const Batch& batch) const {
    std::vector<std::string> result(batch.RowsCount());
    std::vector<bool> condition(batch.RowsCount(), true);
    for (size_t i = 0; i < condition_column_names.size(); ++i) {
        const auto [column_type, column_index] =
            queries_executor_detail::ResolveColumn(batch.GetSchema(), condition_column_names[i], "CaseWhenTransform");
        (void)column_type;
        for (size_t j = 0; j < batch.RowsCount(); ++j) {
            if (!condition[j]) {
                continue;
            }
            if (!batch.ColumnAt(column_index).Compare(condition_values[i], j, condition_signs[i])) {
                condition[j] = false;
            }
        }
    }
    const Type result_type = ResultType(batch.GetSchema());
    const bool has_true_column = batch.GetSchema().HasColumn(column_true);
    const bool has_false_column = batch.GetSchema().HasColumn(column_false);

    const Column* true_column = nullptr;
    if (has_true_column) {
        const auto [input_type, column_index] =
            queries_executor_detail::ResolveColumn(batch.GetSchema(), column_true, "CaseWhenTransform");
        (void)input_type;
        true_column = &batch.ColumnAt(column_index);
    }

    const Column* false_column = nullptr;
    if (has_false_column) {
        const auto [input_type, column_index] =
            queries_executor_detail::ResolveColumn(batch.GetSchema(), column_false, "CaseWhenTransform");
        (void)input_type;
        false_column = &batch.ColumnAt(column_index);
    }

    for (size_t j = 0; j < batch.RowsCount(); ++j) {
        if (condition[j]) {
            result[j] = true_column ? true_column->GetElemToString(j) : column_true;
            continue;
        }
        result[j] = false_column ? false_column->GetElemToString(j) : column_false;
    }

    return CreateColumn(result_type, result);
}
