#include "queries_executor/transform.h"
#include "engine/data_storage/column.h"
#include "queries_executor/helpers.h"
#include <re2/re2.h>
#include <utils.h>
#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace {

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

template <typename ColumnT>
std::vector<int64_t> CopyAddInt64(const Batch& batch, size_t column_index, int64_t value) {
    const auto& int_column = GetTypedColumn<ColumnT>(batch, column_index, "int", "AddTransform");
    const auto& data = int_column.Data();
    std::vector<int64_t> result(data.size());
    for (size_t j = 0; j < data.size(); ++j) {
        result[j] = static_cast<int64_t>(data[j]) + value;
    }
    return result;
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
    if (column_index_mem == SIZE_MAX) {
        column_index_mem = ExpectSourceType(input_schema, source_column_name, Type::timestamp, "TIMESTAMP", "ExtractMinuteTransform");
    }
    return Type::int64;
}

std::shared_ptr<Column> ExtractMinuteTransform::Apply(const Batch& batch) const {
    if (column_index_mem == SIZE_MAX) {
        column_index_mem = ExpectSourceType(batch.GetSchema(), source_column_name, Type::timestamp, "TIMESTAMP", "ExtractMinuteTransform");
    }
    const auto& timestamp_column =
        GetTypedColumn<TimeStampColumn>(batch, column_index_mem, "TIMESTAMP", "ExtractMinuteTransform");

    const auto& data = timestamp_column.Data();
    std::vector<int64_t> minutes(batch.RowsCount(), 0);
    for (size_t j = 0; j < batch.RowsCount(); ++j) {
        minutes[j] = data[j].m;
    }
    return std::make_shared<Int64Column>(std::move(minutes));
}

DateTruncMinuteTransform::DateTruncMinuteTransform(const std::string& source_column_name_, const std::string& result_name_)
    : Transform(result_name_), source_column_name(source_column_name_) {
    if (result_name_.empty()) {
        result_name = "DATE_TRUNC('minute', " + source_column_name_ + ")";
    }
}

Type DateTruncMinuteTransform::ResultType(const Schema& input_schema) const {
    if (column_index_mem == SIZE_MAX) {
        column_index_mem = ExpectSourceType(input_schema, source_column_name, Type::timestamp, "TIMESTAMP", "DateTruncMinuteTransform");
    }
    return Type::timestamp;
}

std::shared_ptr<Column> DateTruncMinuteTransform::Apply(const Batch& batch) const {
    if (column_index_mem == SIZE_MAX) {
        column_index_mem = ExpectSourceType(batch.GetSchema(), source_column_name, Type::timestamp, "TIMESTAMP", "DateTruncMinuteTransform");
    }
    const auto& timestamp_column =
        GetTypedColumn<TimeStampColumn>(batch, column_index_mem, "TIMESTAMP", "DateTruncMinuteTransform");

    const auto& data = timestamp_column.Data();
    std::vector<TimeStamp> values(batch.RowsCount());
    for (size_t j = 0; j < batch.RowsCount(); ++j) {
        values[j] = data[j];
        values[j].s = 0;
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
    if (column_index_mem == SIZE_MAX) {
        column_index_mem = ExpectSourceType(input_schema, source_column_name, Type::str, "STRING", "LengthTransform");
    }
    return Type::int64;
}

std::shared_ptr<Column> LengthTransform::Apply(const Batch& batch) const {
    if (column_index_mem == SIZE_MAX) {
        column_index_mem = ExpectSourceType(batch.GetSchema(), source_column_name, Type::str, "STRING", "LengthTransform");
    }
    const auto& str_column = GetTypedColumn<StrColumn>(batch, column_index_mem, "STRING", "LengthTransform");
    std::vector<int64_t> lengths(batch.RowsCount(), 0);
    for (size_t j = 0; j < batch.RowsCount(); ++j) {
        lengths[j] = static_cast<int64_t>(str_column.GetElemView(j).size());
    }
    return std::make_shared<Int64Column>(std::move(lengths));
}


RegexpReplaceTransform::RegexpReplaceTransform(const std::string& source_column_name_, const std::string& pattern_, const std::string& replacement_, const std::string& result_name_)
    : Transform(result_name_), source_column_name(source_column_name_), regex_pattern(pattern_), replacement(replacement_) {
    if (result_name_.empty()) {
        result_name = "regexp_replace(" + source_column_name_ + ", '" + pattern_ + "', '" + replacement_ + "')";
    }
}

Type RegexpReplaceTransform::ResultType(const Schema& input_schema) const {
    if (column_index_mem == SIZE_MAX) {
        column_index_mem = ExpectSourceType(input_schema, source_column_name, Type::str, "STRING", "RegexpReplaceTransform");
    }
    return Type::str;
}

std::shared_ptr<Column> RegexpReplaceTransform::Apply(const Batch& batch) const {
    if (column_index_mem == SIZE_MAX) {
        column_index_mem = ExpectSourceType(batch.GetSchema(), source_column_name, Type::str, "STRING", "RegexpReplaceTransform");
    }
    const auto& str_column =
        GetTypedColumn<StrColumn>(batch, column_index_mem, "STRING", "RegexpReplaceTransform");
    const size_t n = batch.RowsCount();
    std::vector<std::string> values(n);
    if (batch.HasMask()) {
        const auto& banned = batch.banned_rows;
        for (size_t j = 0; j < n; ++j) {
            if (banned[j]) continue;
            const auto view = str_column.GetElemView(j);
            values[j].assign(view.data(), view.size());
            RE2::GlobalReplace(&values[j], regex_pattern, replacement);
        }
    } else {
        for (size_t j = 0; j < n; ++j) {
            const auto view = str_column.GetElemView(j);
            values[j].assign(view.data(), view.size());
            RE2::GlobalReplace(&values[j], regex_pattern, replacement);
        }
    }
    return std::make_shared<StrColumn>(std::move(values));
}


AddTransform::AddTransform(const std::string& source_column_name_, int64_t value_, const std::string& result_name_): Transform(result_name_), source_column_name(source_column_name_), value(value_) {
    if (result_name_.empty()) {
        result_name = source_column_name_ + " + " + std::to_string(value_);
    }
}

Type AddTransform::ResultType(const Schema& input_schema) const {
    if (column_index_mem == SIZE_MAX) {
        const auto [input_type, column_index] = queries_executor_detail::ResolveColumn(input_schema, source_column_name, "AddTransform");
        if (allowed_types.find(input_type) == allowed_types.end()) {
            throw std::runtime_error(
                "AddTransform expects integer column as source, got " + std::string(TypeToString(input_type)) + " for column " + source_column_name
            );
        }
        column_index_mem = column_index;
        input_type_mem = input_type;
    }
    return Type::int64;
}

std::shared_ptr<Column>  AddTransform::Apply(const Batch& batch) const {
    if (column_index_mem == SIZE_MAX) {
        ResultType(batch.GetSchema());
    }
    std::vector<int64_t> result;
    switch (input_type_mem) {
        case Type::int64:
            result = CopyAddInt64<Int64Column>(batch, column_index_mem, value);
            break;
        case Type::int32:
            result = CopyAddInt64<Int32Column>(batch, column_index_mem, value);
            break;
        case Type::int16:
            result = CopyAddInt64<Int16Column>(batch, column_index_mem, value);
            break;
        case Type::int8:
            result = CopyAddInt64<Int8Column>(batch, column_index_mem, value);
            break;
        default:
            throw std::runtime_error("unexpected type for AddTransform source column");
    }
    return std::make_shared<Int64Column>(std::move(result));
}

SubTransform::SubTransform(const std::string& source_column_name_, int64_t value_, const std::string& result_name_) : AddTransform(source_column_name_, -value_, result_name_) {
    if (result_name_.empty()) {
        result_name = source_column_name_ + " - " + std::to_string(value_);
    }
}


ConstantInt8Transform::ConstantInt8Transform(const int8_t& value_, const std::string& result_name_) : Transform(result_name_), value(value_) {
    if (result_name_.empty()) {
        result_name = std::to_string(value_);
    }
}

Type ConstantInt8Transform::ResultType(const Schema&) const {
    return Type::int8;
}

std::shared_ptr<Column> ConstantInt8Transform::Apply(const Batch& batch) const {
    std::vector<int8_t> values(batch.RowsCount(), value);
    return std::make_shared<Int8Column>(std::move(values));
}


RenameTransform::RenameTransform(const std::string& source_column_name_, const std::string& result_name_)
    : Transform(result_name_), source_column_name(source_column_name_) {
    if (result_name_.empty()) {
        result_name = source_column_name_;
    }
}

Type RenameTransform::ResultType(const Schema& input_schema) const {
    if (column_index_mem == SIZE_MAX) {
        const auto [input_type, column_index] =
            queries_executor_detail::ResolveColumn(input_schema, source_column_name, "RenameTransform");
        column_index_mem = column_index;
        return input_type;
    }
    return input_schema.ColumnTypeAt(column_index_mem);
}

std::shared_ptr<Column> RenameTransform::Apply(const Batch& batch) const {
    if (column_index_mem == SIZE_MAX) {
        column_index_mem = queries_executor_detail::ResolveColumn(batch.GetSchema(), source_column_name, "RenameTransform").second;
    }
    return batch.ColumnSharedAt(column_index_mem);
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
    if (batch.HasMask()) {
        for (size_t j = 0; j < batch.RowsCount(); ++j) {
            if (batch.banned_rows[j]) condition[j] = false;
        }
    }
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
        if (batch.HasMask() && batch.banned_rows[j]) continue;
        if (condition[j]) {
            result[j] = true_column ? true_column->GetElemToString(j) : column_true;
            continue;
        }
        result[j] = false_column ? false_column->GetElemToString(j) : column_false;
    }

    return CreateColumn(result_type, result);
}
