#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/numeric_visitor.h"
#include "queries_executor/helpers.h"
#include <queries_executor/aggregation.h>
#include <stdexcept>



CountAggregation::CountAggregation(std::string result_name_) : Aggregation("", result_name_){
    if (result_name.empty()) {
        result_name = "COUNT(*)";
    }
}

void CountAggregation::RunBatch(std::shared_ptr<Batch> batch, const std::vector<size_t>& group_indices) {
    if (!batch) {
        return;
    }
    size_t max_ind = 0;
    for (auto& group_index : group_indices) {
        if (group_index != SIZE_MAX && group_index > max_ind) {
            max_ind = group_index;
        }
    }
    if (max_ind + 1 > rows_count.size()) {
        rows_count.resize(max_ind + 1, 0);
    }
    for (auto& group_ind : group_indices) {
        if (group_ind == SIZE_MAX) {
            continue;
        }
        ++rows_count[group_ind];
    }
}

// void CountAggregation::RunRow(std::shared_ptr<Batch> batch, size_t row_index) {
//     queries_executor_detail::CheckRow(batch, row_index);
//     ++rows_count;
// }

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


SumAggregation::SumAggregation(const std::string col_name, std::string result_name_)
    : Aggregation(col_name, result_name_), visitor() {
    if (result_name.empty()) {
        result_name = "SUM(" + col_name + ")";
    }
}

void SumAggregation::RunBatch(std::shared_ptr<Batch> batch, const std::vector<size_t>& group_indices) {
    if (!batch) {
        return;
    }
    const auto [column_type, column_index] =
        queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "SumAggregation");
    if (!input_type.has_value()) {
        input_type = column_type;
    } else if (input_type.value() != column_type) {
        throw std::runtime_error("sum aggregation got different column types across batches");
    }
    batch->ColumnAt(column_index).Accept(visitor, group_indices);
}

// void SumAggregation::RunRow(std::shared_ptr<Batch> batch, size_t row_index) {
//     queries_executor_detail::CheckRow(batch, row_index);
//     if (column_index == -1u) {
//         column_index = queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "SumAggregation").second;
//     }
//     batch->ColumnAt(column_index).Accept(visitor, row_index);
// }

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


AvgAggregation::AvgAggregation(const std::string col_name, std::string result_name_) : Aggregation(col_name, result_name_), visitor() {
    if (result_name.empty()) {
        result_name = "AVG(" + col_name + ")";
    }
}

void AvgAggregation::RunBatch(std::shared_ptr<Batch> batch, const std::vector<size_t>& group_indices) {
    if (!batch) {
        return;
    }
    const auto [column_type, column_index] =
        queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "AvgAggregation");
    if (!input_type.has_value()) {
        input_type = column_type;
    } else if (input_type.value() != column_type) {
        throw std::runtime_error("avg aggregation got different column types across batches");
    }
    batch->ColumnAt(column_index).Accept(visitor, group_indices);
}

// void AvgAggregation::RunRow(std::shared_ptr<Batch> batch, size_t row_index) {
//     queries_executor_detail::CheckRow(batch, row_index);
//     if (column_index == -1u) {
//         column_index = queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "AvgAggregation").second;
//     }
//     batch->ColumnAt(column_index).Accept(visitor, row_index);
// }

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



CountDistinctAggregation::CountDistinctAggregation(const std::string col_name, std::string result_name_) : Aggregation(col_name, result_name_){
    if (result_name.empty()) {
        result_name = "COUNT(DISTINCT " + col_name + ")";
    }
}

void CountDistinctAggregation::RunBatch(std::shared_ptr<Batch> batch, const std::vector<size_t>& group_indices) {
    if (!batch) {
        return;
    }
    const auto [column_type, column_index] = queries_executor_detail::ResolveColumn(
        batch->GetSchema(),
        column_name,
        "CountDistinctAggregation"
    );
    (void)column_type;
    batch->ColumnAt(column_index).Accept(visitor, group_indices);
}

// void CountDistinctAggregation::RunRow(std::shared_ptr<Batch> batch, size_t row_index) {
//     queries_executor_detail::CheckRow(batch, row_index);
//     if (column_index == -1u) {
//         column_index = queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "CountDistinctAggregation").second;
//     }
//     batch->ColumnAt(column_index).Accept(visitor, row_index);
// }

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



MaxAggregation::MaxAggregation(const std::string col_name, std::string result_name_) : Aggregation(col_name, result_name_), numeric_visitor(), date_visitor() {
    if (result_name.empty()) {
        result_name = "MAX(" + col_name + ")";
    }
}

void MaxAggregation::RunBatch(std::shared_ptr<Batch> batch, const std::vector<size_t>& group_indices) {
    if (!batch) {
        return;
    }
    const auto [column_type, column_index] =
        queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "MaxAggregation");
    if (!input_type.has_value()) {
        input_type = column_type;
    } else if (input_type.value() != column_type) {
        throw std::runtime_error("max aggregation got different column types across batches");
    }
    if (input_type.value() == Type::str) {
        batch->ColumnAt(column_index).Accept(str_visitor, group_indices);
        return;
    }
    if (input_type.value() == Type::date || input_type.value() == Type::timestamp) {
        batch->ColumnAt(column_index).Accept(date_visitor, group_indices);
        return;
    }
    batch->ColumnAt(column_index).Accept(numeric_visitor, group_indices);
}

// void MaxAggregation::RunRow(std::shared_ptr<Batch> batch, size_t row_index) {
//     queries_executor_detail::CheckRow(batch, row_index);
//     if (column_index == -1u) {
//         const auto [type, idx] = queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "MaxAggregation");
//         column_index = idx;
//         input_type = type;
//     }
//     if (input_type.value() == Type::date || input_type.value() == Type::timestamp || input_type.value() == Type::str) {
//         batch->ColumnAt(column_index).Accept(date_visitor, row_index);
//         return;
//     }
//     batch->ColumnAt(column_index).Accept(numeric_visitor, row_index);
// }

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


MinAggregation::MinAggregation(const std::string col_name, std::string result_name_) : Aggregation(col_name, result_name_), numeric_visitor(), date_visitor() {
    if (result_name.empty()) {
        result_name = "MIN(" + col_name + ")";
    }
}

void MinAggregation::RunBatch(std::shared_ptr<Batch> batch, const std::vector<size_t>& group_indices) {
    if (!batch) {
        return;
    }
    const auto [column_type, column_index] =
        queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "MinAggregation");
    if (!input_type.has_value()) {
        input_type = column_type;
    } else if (input_type.value() != column_type) {
        throw std::runtime_error("min aggregation got different column types across batches");
    }
    if (input_type.value() == Type::str) {
        batch->ColumnAt(column_index).Accept(str_visitor, group_indices);
        return;
    }
    if (input_type.value() == Type::date || input_type.value() == Type::timestamp) {
        batch->ColumnAt(column_index).Accept(date_visitor, group_indices);
        return;
    }
    batch->ColumnAt(column_index).Accept(numeric_visitor, group_indices);
}

// void MinAggregation::RunRow(std::shared_ptr<Batch> batch, size_t row_index) {
//     queries_executor_detail::CheckRow(batch, row_index);
//     if (column_index == -1u) {
//         const auto [type, idx] = queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "MinAggregation");
//         column_index = idx;
//         input_type = type;
//     }
//     if (input_type.value() == Type::date || input_type.value() == Type::timestamp || input_type.value() == Type::str) {
//         batch->ColumnAt(column_index).Accept(date_visitor, row_index);
//         return;
//     }
//     batch->ColumnAt(column_index).Accept(numeric_visitor, row_index);
// }

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
