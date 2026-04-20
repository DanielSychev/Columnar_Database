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

    auto column = std::make_shared<Int64Column>();
    column->AddElem(std::to_string(visitor.IntegralSum()));
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
    auto column = std::make_shared<DoubleColumn>();
    column->AddElem(std::to_string(visitor.Avg()));
    return column;
}
