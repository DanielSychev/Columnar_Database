#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/numeric_visitor.h"
#include <queries_executor/aggregation.h>
#include <stdexcept>

SumAggregation::SumAggregation(const std::string col_name) : column_name(col_name), visitor() {
}

void SumAggregation::RunBatch(std::shared_ptr<Batch> batch) {
    if (!batch) {
        // throw std::runtime_error("expected batch");
        return;
    }
    size_t column_index;
    if (auto type = batch->GetSchema().GetTypeAndPos(column_name); type.has_value()) {
        if (!input_type.has_value()) {
            input_type = type->first;
        } else if (input_type.value() != type->first) {
            throw std::runtime_error("sum aggregation got different column types across batches");
        }
        column_index = type->second;
    } else {
        throw std::runtime_error("no such column in schema");
    }
    batch->ColumnAt(column_index).Accept(visitor);
}

std::shared_ptr<Column> SumAggregation::GetResult() const {
    if (input_type.has_value() && input_type.value() == Type::double_) {
        auto column = std::make_shared<DoubleColumn>();
        column->AddElem(std::to_string(visitor.DoubleSum()));
        return column;
    }

    auto column = std::make_shared<Int128Column>();
    column->AddElem(column_detail::ToString(visitor.IntegralSum()));
    return column;
}




void CountAggregation::RunBatch(std::shared_ptr<Batch> batch) {
    if (!batch) {
        return;
    }
    rows_count += batch->RowsCount();
}

std::shared_ptr<Column> CountAggregation::GetResult() const {
    auto column = std::make_shared<Int64Column>();
    column->AddElem(std::to_string(rows_count));
    return column;
}



AvgAggregation::AvgAggregation(const std::string col_name) : column_name(col_name), visitor() {
}

void AvgAggregation::RunBatch(std::shared_ptr<Batch> batch) {
    if (!batch) {
        // throw std::runtime_error("expected batch");
        return;
    }
    size_t column_index;
    if (auto type = batch->GetSchema().GetTypeAndPos(column_name); type.has_value()) {
        if (!input_type.has_value()) {
            input_type = type->first;
        } else if (input_type.value() != type->first) {
            throw std::runtime_error("avg aggregation got different column types across batches");
        }
        column_index = type->second;
    } else {
        throw std::runtime_error("no such column in schema");
    }
    batch->ColumnAt(column_index).Accept(visitor);
}

std::shared_ptr<Column> AvgAggregation::GetResult() const {
    auto column = std::make_shared<Int64Column>();
    column->AddElem(std::to_string(visitor.Avg()));
    return column;
}

CountDistinctAggregation::CountDistinctAggregation(const std::string col_name) : column_name(col_name) {
}

void CountDistinctAggregation::RunBatch(std::shared_ptr<Batch> batch) {
    if (!batch) {
        return;
    }
    size_t column_index;
    if (auto type = batch->GetSchema().GetTypeAndPos(column_name); type.has_value()) {
        column_index = type->second;
    } else {
        throw std::runtime_error("no such column in schema");
    }
    auto& column = batch->ColumnAt(column_index);
    for (size_t i = 0; i < batch->RowsCount(); ++i) {
        distinct_values.insert(column.GetElemToString(i));
    }
}

std::shared_ptr<Column> CountDistinctAggregation::GetResult() const {
    auto column = std::make_shared<Int64Column>();
    column->AddElem(std::to_string(distinct_values.size()));
    return column;
}



MaxAggregation::MaxAggregation(const std::string col_name) : column_name(col_name), numeric_visitor(), date_visitor() {
}

void MaxAggregation::RunBatch(std::shared_ptr<Batch> batch) {
    if (!batch) {
        // throw std::runtime_error("expected batch");
        return;
    }
    size_t column_index;
    if (auto type = batch->GetSchema().GetTypeAndPos(column_name); type.has_value()) {
        if (!input_type.has_value()) {
            input_type = type->first;
        } else if (input_type.value() != type->first) {
            throw std::runtime_error("max aggregation got different column types across batches");
        }
        column_index = type->second;
    } else {
        throw std::runtime_error("no such column in schema");
    }
    if (input_type.value() == Type::date || input_type.value() == Type::timestamp) {
        batch->ColumnAt(column_index).Accept(date_visitor);
        return;
    }
    batch->ColumnAt(column_index).Accept(numeric_visitor);
}

std::shared_ptr<Column> MaxAggregation::GetResult() const {
    if (input_type.has_value() && (input_type.value() == Type::date || input_type.value() == Type::timestamp)) {
        auto column = std::make_shared<StrColumn>();
        column->AddElem(std::string(date_visitor.Max()));
        return column;
    }

    auto column = std::make_shared<Int64Column>();
    column->AddElem(std::to_string(numeric_visitor.Max()));
    return column;
}


MinAggregation::MinAggregation(const std::string col_name) : column_name(col_name), numeric_visitor(), date_visitor() {
}

void MinAggregation::RunBatch(std::shared_ptr<Batch> batch) {
    if (!batch) {
        // throw std::runtime_error("expected batch");
        return;
    }
    size_t column_index;
    if (auto type = batch->GetSchema().GetTypeAndPos(column_name); type.has_value()) {
        if (!input_type.has_value()) {
            input_type = type->first;
        } else if (input_type.value() != type->first) {
            throw std::runtime_error("min aggregation got different column types across batches");
        }
        column_index = type->second;
    } else {
        throw std::runtime_error("no such column in schema");
    }
    if (input_type.value() == Type::date || input_type.value() == Type::timestamp) {
        batch->ColumnAt(column_index).Accept(date_visitor);
        return;
    }
    batch->ColumnAt(column_index).Accept(numeric_visitor);
}

std::shared_ptr<Column> MinAggregation::GetResult() const {
    if (input_type.has_value() && (input_type.value() == Type::date || input_type.value() == Type::timestamp)) {
        auto column = std::make_shared<StrColumn>();
        column->AddElem(std::string(date_visitor.Min()));
        return column;
    }

    auto column = std::make_shared<Int64Column>();
    column->AddElem(std::to_string(numeric_visitor.Min()));
    return column;
}