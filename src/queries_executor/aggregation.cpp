#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/numeric_visitor.h"
#include "queries_executor/helpers.h"
#include <queries_executor/aggregation.h>
#include <stdexcept>

CountAggregation::CountAggregation(std::string result_name_) : Aggregation(result_name_){
    if (result_name.empty()) {
        result_name = "COUNT(*)";
    }
}

void CountAggregation::RunBatch(std::shared_ptr<Batch> batch) {
    if (!batch) {
        return;
    }
    rows_count += batch->RowsCount();
}

std::string CountAggregation::GetResultValue() const {
    return std::to_string(rows_count);
}

Type CountAggregation::GetResultType() const {
    return Type::int64;
}

std::shared_ptr<Aggregation> CountAggregation::Clone() const {
    return std::make_shared<CountAggregation>(result_name);
}


SumAggregation::SumAggregation(const std::string col_name, std::string result_name_)
    : Aggregation(result_name_), column_name(col_name), visitor() {
    if (result_name.empty()) {
        result_name = "SUM(" + col_name + ")";
    }
}

void SumAggregation::RunBatch(std::shared_ptr<Batch> batch) {
    if (!batch) {
        // throw std::runtime_error("expected batch");
        return;
    }
    const auto [column_type, column_index] =
        queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "SumAggregation");
    if (!input_type.has_value()) {
        input_type = column_type;
    } else if (input_type.value() != column_type) {
        throw std::runtime_error("sum aggregation got different column types across batches");
    }
    batch->ColumnAt(column_index).Accept(visitor);
}

std::string SumAggregation::GetResultValue() const {
    if (input_type.has_value() && input_type.value() == Type::double_) {
        return std::to_string(visitor.DoubleSum());
    }

    return column_detail::ToString(visitor.IntegralSum());
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


AvgAggregation::AvgAggregation(const std::string col_name, std::string result_name_) : Aggregation(result_name_), column_name(col_name), visitor() {
    if (result_name.empty()) {
        result_name = "AVG(" + col_name + ")";
    }
}

void AvgAggregation::RunBatch(std::shared_ptr<Batch> batch) {
    if (!batch) {
        // throw std::runtime_error("expected batch");
        return;
    }
    const auto [column_type, column_index] =
        queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "AvgAggregation");
    if (!input_type.has_value()) {
        input_type = column_type;
    } else if (input_type.value() != column_type) {
        throw std::runtime_error("avg aggregation got different column types across batches");
    }
    batch->ColumnAt(column_index).Accept(visitor);
}

std::string AvgAggregation::GetResultValue() const {
    return std::to_string(visitor.Avg());
}

Type AvgAggregation::GetResultType() const {
    return Type::int64;
}

std::shared_ptr<Aggregation> AvgAggregation::Clone() const {
    return std::make_shared<AvgAggregation>(column_name, result_name);
}



CountDistinctAggregation::CountDistinctAggregation(const std::string col_name, std::string result_name_) : Aggregation(result_name_), column_name(col_name) {
    if (result_name.empty()) {
        result_name = "COUNT(DISTINCT " + col_name + ")";
    }
}

void CountDistinctAggregation::RunBatch(std::shared_ptr<Batch> batch) {
    if (!batch) {
        return;
    }
    const auto [column_type, column_index] = queries_executor_detail::ResolveColumn(
        batch->GetSchema(),
        column_name,
        "CountDistinctAggregation"
    );
    (void)column_type;
    auto& column = batch->ColumnAt(column_index);
    for (size_t i = 0; i < batch->RowsCount(); ++i) {
        distinct_values.insert(column.GetElemToString(i));
    }
}

std::string CountDistinctAggregation::GetResultValue() const {
    return std::to_string(distinct_values.size());
}

Type CountDistinctAggregation::GetResultType() const {
    return Type::int64;
}

std::shared_ptr<Aggregation> CountDistinctAggregation::Clone() const {
    return std::make_shared<CountDistinctAggregation>(column_name, result_name);
}



MaxAggregation::MaxAggregation(const std::string col_name, std::string result_name_) : Aggregation(result_name_), column_name(col_name), numeric_visitor(), date_visitor() {
    if (result_name.empty()) {
        result_name = "MAX(" + col_name + ")";
    }
}

void MaxAggregation::RunBatch(std::shared_ptr<Batch> batch) {
    if (!batch) {
        // throw std::runtime_error("expected batch");
        return;
    }
    const auto [column_type, column_index] =
        queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "MaxAggregation");
    if (!input_type.has_value()) {
        input_type = column_type;
    } else if (input_type.value() != column_type) {
        throw std::runtime_error("max aggregation got different column types across batches");
    }
    if (input_type.value() == Type::date || input_type.value() == Type::timestamp || input_type.value() == Type::str) {
        batch->ColumnAt(column_index).Accept(date_visitor);
        return;
    }
    batch->ColumnAt(column_index).Accept(numeric_visitor);
}

std::string MaxAggregation::GetResultValue() const {
    if (input_type.has_value() && (input_type.value() == Type::date || input_type.value() == Type::timestamp || input_type.value() == Type::str)) {
        return std::string(date_visitor.Max());
    }

    return std::to_string(numeric_visitor.Max());
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



MinAggregation::MinAggregation(const std::string col_name, std::string result_name_) : Aggregation(result_name_), column_name(col_name), numeric_visitor(), date_visitor() {
    if (result_name.empty()) {
        result_name = "MIN(" + col_name + ")";
    }
}

void MinAggregation::RunBatch(std::shared_ptr<Batch> batch) {
    if (!batch) {
        // throw std::runtime_error("expected batch");
        return;
    }
    const auto [column_type, column_index] =
        queries_executor_detail::ResolveColumn(batch->GetSchema(), column_name, "MinAggregation");
    if (!input_type.has_value()) {
        input_type = column_type;
    } else if (input_type.value() != column_type) {
        throw std::runtime_error("min aggregation got different column types across batches");
    }
    if (input_type.value() == Type::date || input_type.value() == Type::timestamp || input_type.value() == Type::str) {
        batch->ColumnAt(column_index).Accept(date_visitor);
        return;
    }
    batch->ColumnAt(column_index).Accept(numeric_visitor);
}

std::string MinAggregation::GetResultValue() const {
    if (input_type.has_value() && (input_type.value() == Type::date || input_type.value() == Type::timestamp || input_type.value() == Type::str)) {
        return std::string(date_visitor.Min());
    }

    return std::to_string(numeric_visitor.Min());
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
