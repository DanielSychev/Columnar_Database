#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/numeric_visitor.h"
#include "queries_executor/helpers.h"
#include <queries_executor/aggregation.h>
#include <stdexcept>
#include <utility>



CountAggregation::CountAggregation(std::string result_name_) : Aggregation("", std::move(result_name_)){
    if (result_name.empty()) {
        result_name = "COUNT(*)";
    }
}

void CountAggregation::RunBatch(const std::shared_ptr<Batch>& batch, const std::vector<size_t>& group_indices, size_t max_group_index) {
    if (!batch) {
        return;
    }
    if (max_group_index + 1 > rows_count.size()) {
        rows_count.resize(max_group_index + 1, 0);
    }
    for (auto& group_ind : group_indices) {
        if (group_ind == SIZE_MAX) {
            continue;
        }
        ++rows_count[group_ind];
    }
}

std::string CountAggregation::GetResultValue(size_t group_index) const {
    return std::to_string(rows_count[group_index]);
}

void CountAggregation::GetResultInto(Column& column, size_t group_index) const {
    static_cast<Int64Column&>(column).AppendRaw(static_cast<int64_t>(rows_count[group_index]));
}

Type CountAggregation::GetResultType() const {
    return Type::int64;
}

std::shared_ptr<Aggregation> CountAggregation::Clone() const {
    return std::make_shared<CountAggregation>(result_name);
}


SumAggregation::SumAggregation(std::string col_name, std::string result_name_)
    : Aggregation(std::move(col_name), std::move(result_name_)) {
    if (result_name.empty()) {
        result_name = "SUM(" + column_name + ")";
    }
}

void SumAggregation::RunBatch(const std::shared_ptr<Batch>& batch, const std::vector<size_t>& group_indices, size_t max_group_index) {
    if (!batch) {
        return;
    }
    if (column_index_cache == SIZE_MAX) {
        const auto [column_type, column_index] =
            queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "SumAggregation");
        column_index_cache = column_index;
        input_type = column_type;
    }
    batch->ColumnAt(column_index_cache).Accept(visitor, group_indices, max_group_index);
}

std::string SumAggregation::GetResultValue(size_t group_index) const {
    if (input_type.has_value() && input_type.value() == Type::double_) {
        return std::to_string(visitor.DoubleSum(group_index));
    }

    return column_detail::ToString(visitor.IntegralSum(group_index));
}

void SumAggregation::GetResultInto(Column& column, size_t group_index) const {
    if (input_type.has_value() && input_type.value() == Type::double_) {
        static_cast<DoubleColumn&>(column).AppendRaw(visitor.DoubleSum(group_index));
        return;
    }
    static_cast<Int128Column&>(column).AppendRaw(visitor.IntegralSum(group_index));
}

Type SumAggregation::GetResultType() const {
    if (input_type.has_value() && input_type.value() == Type::double_) {
        return Type::double_;
    }
    return Type::int128;
}

std::shared_ptr<Aggregation> SumAggregation::Clone() const {
    return std::make_shared<SumAggregation>(column_name, result_name);
}


AvgAggregation::AvgAggregation(std::string col_name, std::string result_name_)
    : Aggregation(std::move(col_name), std::move(result_name_)) {
    if (result_name.empty()) {
        result_name = "AVG(" + column_name + ")";
    }
}

void AvgAggregation::RunBatch(const std::shared_ptr<Batch>& batch, const std::vector<size_t>& group_indices, size_t max_group_index) {
    if (!batch) {
        return;
    }
    if (column_index_cache == SIZE_MAX) {
        const auto [column_type, column_index] =
            queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "AvgAggregation");
        column_index_cache = column_index;
        input_type = column_type;
    }
    batch->ColumnAt(column_index_cache).Accept(visitor, group_indices, max_group_index);
}

std::string AvgAggregation::GetResultValue(size_t group_index) const {
    return std::to_string(visitor.Avg(group_index));
}

void AvgAggregation::GetResultInto(Column& column, size_t group_index) const {
    static_cast<Int64Column&>(column).AppendRaw(visitor.Avg(group_index));
}

Type AvgAggregation::GetResultType() const {
    return Type::int64;
}

std::shared_ptr<Aggregation> AvgAggregation::Clone() const {
    return std::make_shared<AvgAggregation>(column_name, result_name);
}



CountDistinctAggregation::CountDistinctAggregation(std::string col_name, std::string result_name_)
    : Aggregation(std::move(col_name), std::move(result_name_)) {
    if (result_name.empty()) {
        result_name = "COUNT(DISTINCT " + column_name + ")";
    }
}

void CountDistinctAggregation::RunBatch(const std::shared_ptr<Batch>& batch, const std::vector<size_t>& group_indices, size_t max_group_index) {
    if (!batch) {
        return;
    }
    if (column_index_cache == SIZE_MAX) {
        column_index_cache = queries_executor_detail::ResolveColumn(
            batch->GetSchema(),
            column_name,
            "CountDistinctAggregation"
        ).second;
    }
    batch->ColumnAt(column_index_cache).Accept(visitor, group_indices, max_group_index);
}

std::string CountDistinctAggregation::GetResultValue(size_t group_index) const {
    return std::to_string(visitor.Count(group_index));
}

void CountDistinctAggregation::GetResultInto(Column& column, size_t group_index) const {
    static_cast<Int64Column&>(column).AppendRaw(static_cast<int64_t>(visitor.Count(group_index)));
}

Type CountDistinctAggregation::GetResultType() const {
    return Type::int64;
}

std::shared_ptr<Aggregation> CountDistinctAggregation::Clone() const {
    return std::make_shared<CountDistinctAggregation>(column_name, result_name);
}



MaxAggregation::MaxAggregation(std::string col_name, std::string result_name_)
    : Aggregation(std::move(col_name), std::move(result_name_)) {
    if (result_name.empty()) {
        result_name = "MAX(" + column_name + ")";
    }
}

void MaxAggregation::RunBatch(const std::shared_ptr<Batch>& batch, const std::vector<size_t>& group_indices, size_t max_group_index) {
    if (!batch) {
        return;
    }
    if (column_index_cache == SIZE_MAX) {
        const auto [column_type, column_index] =
            queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "MaxAggregation");
        column_index_cache = column_index;
        input_type = column_type;
    }
    if (input_type.value() == Type::str) {
        batch->ColumnAt(column_index_cache).Accept(str_visitor, group_indices, max_group_index);
        return;
    }
    if (input_type.value() == Type::date || input_type.value() == Type::timestamp) {
        batch->ColumnAt(column_index_cache).Accept(date_visitor, group_indices, max_group_index);
        return;
    }
    batch->ColumnAt(column_index_cache).Accept(numeric_visitor, group_indices, max_group_index);
}

std::string MaxAggregation::GetResultValue(size_t group_index) const {
    if (input_type.has_value() && input_type.value() == Type::date)
        return DateToString(Int32ToDate(date_visitor.MaxDate(group_index)));
    if (input_type.has_value() && input_type.value() == Type::timestamp)
        return TimeStampToString(Int64ToTimeStamp(date_visitor.MaxTimestamp(group_index)));
    if (input_type.has_value() && input_type.value() == Type::str)
        return str_visitor.MaxStr(group_index);
    return std::to_string(numeric_visitor.Max(group_index));
}

void MaxAggregation::GetResultInto(Column& column, size_t group_index) const {
    if (input_type.has_value() && input_type.value() == Type::date) {
        static_cast<DateColumn&>(column).AppendRaw(Int32ToDate(date_visitor.MaxDate(group_index)));
        return;
    }
    if (input_type.has_value() && input_type.value() == Type::timestamp) {
        static_cast<TimeStampColumn&>(column).AppendRaw(Int64ToTimeStamp(date_visitor.MaxTimestamp(group_index)));
        return;
    }
    if (input_type.has_value() && input_type.value() == Type::str) {
        static_cast<StrColumn&>(column).AppendRaw(str_visitor.MaxStr(group_index));
        return;
    }
    static_cast<Int64Column&>(column).AppendRaw(numeric_visitor.Max(group_index));
}

Type MaxAggregation::GetResultType() const {
    if (input_type.has_value() && (input_type.value() == Type::date || input_type.value() == Type::timestamp || input_type.value() == Type::str)) {
        return input_type.value();
    }
    return Type::int64;
}

std::shared_ptr<Aggregation> MaxAggregation::Clone() const {
    return std::make_shared<MaxAggregation>(column_name, result_name);
}


MinAggregation::MinAggregation(std::string col_name, std::string result_name_)
    : Aggregation(std::move(col_name), std::move(result_name_)) {
    if (result_name.empty()) {
        result_name = "MIN(" + column_name + ")";
    }
}

void MinAggregation::RunBatch(const std::shared_ptr<Batch>& batch, const std::vector<size_t>& group_indices, size_t max_group_index) {
    if (!batch) {
        return;
    }
    if (column_index_cache == SIZE_MAX) {
        const auto [column_type, column_index] =
            queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "MinAggregation");
        column_index_cache = column_index;
        input_type = column_type;
    }
    if (input_type.value() == Type::str) {
        batch->ColumnAt(column_index_cache).Accept(str_visitor, group_indices, max_group_index);
        return;
    }
    if (input_type.value() == Type::date || input_type.value() == Type::timestamp) {
        batch->ColumnAt(column_index_cache).Accept(date_visitor, group_indices, max_group_index);
        return;
    }
    batch->ColumnAt(column_index_cache).Accept(numeric_visitor, group_indices, max_group_index);
}

std::string MinAggregation::GetResultValue(size_t group_index) const {
    if (input_type.has_value() && input_type.value() == Type::date)
        return DateToString(Int32ToDate(date_visitor.MinDate(group_index)));
    if (input_type.has_value() && input_type.value() == Type::timestamp)
        return TimeStampToString(Int64ToTimeStamp(date_visitor.MinTimestamp(group_index)));
    if (input_type.has_value() && input_type.value() == Type::str)
        return str_visitor.MinStr(group_index);
    return std::to_string(numeric_visitor.Min(group_index));
}

void MinAggregation::GetResultInto(Column& column, size_t group_index) const {
    if (input_type.has_value() && input_type.value() == Type::date) {
        static_cast<DateColumn&>(column).AppendRaw(Int32ToDate(date_visitor.MinDate(group_index)));
        return;
    }
    if (input_type.has_value() && input_type.value() == Type::timestamp) {
        static_cast<TimeStampColumn&>(column).AppendRaw(Int64ToTimeStamp(date_visitor.MinTimestamp(group_index)));
        return;
    }
    if (input_type.has_value() && input_type.value() == Type::str) {
        static_cast<StrColumn&>(column).AppendRaw(str_visitor.MinStr(group_index));
        return;
    }
    static_cast<Int64Column&>(column).AppendRaw(numeric_visitor.Min(group_index));
}

Type MinAggregation::GetResultType() const {
    if (input_type.has_value() && (input_type.value() == Type::date || input_type.value() == Type::timestamp || input_type.value() == Type::str)) {
        return input_type.value();
    }
    return Type::int64;
}

std::shared_ptr<Aggregation> MinAggregation::Clone() const {
    return std::make_shared<MinAggregation>(column_name, result_name);
}
