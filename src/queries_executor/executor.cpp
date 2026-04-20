#include "queries_executor/executor.h"
#include "engine/data_storage/batch.h"
// #include "engine/serialization/batch_serialization.h"
#include "queries_executor/operator.h"
#include <memory>
#include <stdexcept>
#include <queries_executor/objects.h>

class ScanExecutor : public PipelineExecutor {
public:
    ScanExecutor(std::shared_ptr<ScanOperator> scan_op) : scan_operator_(std::move(scan_op)) {
        data_reader = &scan_operator_->data_reader;
        size_t pos = data_reader->ReadLastBytes(); // позиция меты
        
        data_reader->SetPos(pos); // читаем схему
        size_t column_count = 0;
        data_reader->BinaryRead(column_count);
        basic_schema.ReadSchema(*data_reader, column_count);
        
        data_reader->BinaryRead(batch_count);
        batch_meta_positions.resize(batch_count);
        for (size_t i = 0; i < batch_meta_positions.size(); ++i) {
            data_reader->BinaryRead(batch_meta_positions[i]);
        }
        
        for (const auto& column_name: scan_operator_->column_names) {
            if (auto type = basic_schema.GetTypeAndPos(column_name); type.has_value()) {
                query_schema.AddColumn(column_name, type->first);
                column_positions.push_back(type->second);
            } else {
                throw std::runtime_error("no such column in basic schema");
            }
        }
        column_starts.resize(basic_schema.NumColums());
    }

    std::shared_ptr<Batch> NextBatch() override {
        if (i >= batch_count) {
            return nullptr;
        }
        data_reader->SetPos(batch_meta_positions[i]);

        for (size_t column_index = 0; column_index < column_starts.size(); ++column_index) {
            data_reader->BinaryRead(column_starts[column_index]);
        }

        auto batch = std::make_shared<Batch>(query_schema, batch_rows_count);
        // if (!batch_serialization::ReadMfBatch(*data_reader, *batch)) {
        //     throw std::runtime_error("wrong batch format");
        // }
        for (size_t i = 0; i < query_schema.NumColums(); ++i) {
            data_reader->SetPos(column_starts[column_positions[i]]);
            Column& column = batch->ColumnAt(i);
            column.Read(*data_reader);
            batch->SetRowsCount(column.Size());
        }
        ++i;
        return batch;
    }

private:
    std::shared_ptr<ScanOperator> scan_operator_;
    Reader* data_reader = nullptr;
    Schema basic_schema, query_schema;
    std::vector<size_t> column_positions, column_starts;
    size_t batch_count, i = 0;
    const size_t batch_rows_count = Constants::BATCH_SIZE;
    std::vector<size_t> batch_meta_positions;
};

// class CountExecutor : public PipelineExecutor {
// public:
//     CountExecutor(std::shared_ptr<CountOperator> count_op) {
//         child_executor_ = ExecuteOperator(count_op->child);
//         if (!child_executor_) {
//             throw std::runtime_error("child executor is not set");
//         }
//     }

//     std::shared_ptr<Batch> NextBatch() override {
//         if (was_produced) {
//             return nullptr;
//         }
//         was_produced = true;
//         size_t rows_count = 0;
//         while (auto batch = child_executor_->NextBatch()) {
//             rows_count += batch->RowsCount();
//         }
//         return CreateConstantBatch(rows_count);
//     }
// private:
//     // std::shared_ptr<CountOperator> count_operator_;
//     std::shared_ptr<PipelineExecutor> child_executor_;
//     bool was_produced = false;
// };

class FilterExecutor : public PipelineExecutor {
public:
    FilterExecutor(std::shared_ptr<FilterOperator> filter_op) : filter_operator_(filter_op) {
        child_executor_ = ExecuteOperator(filter_op->child);
        if (!child_executor_) {
            throw std::runtime_error("child executor is not set");
        }
    }

    std::shared_ptr<Batch> NextBatch() override {
        if (!child_executor_) {
            throw std::runtime_error("child executor is not set");
        }
        auto batch = child_executor_->NextBatch();
        if (!batch) {
            return nullptr;
        }
        size_t column_index;
        if (auto type = batch->GetSchema().GetTypeAndPos(filter_operator_->column_name); type.has_value()) {
            column_index = type->second;
        } else {
            throw std::runtime_error("no such column in schema");
        }
        std::vector<bool> banned(batch->RowsCount(), false);
        for (size_t i = 0; i < batch->RowsCount(); ++i) {
            if (batch->ColumnAt(column_index).Compare(filter_operator_->value, i)) {
                banned[i] = true;
            }
        }
        auto new_batch = std::make_shared<Batch>(batch->GetSchema(), batch->RowsCount());
        for (size_t j = 0; j < batch->ColumnsCount(); ++j) {
            new_batch->AddColumn(j, batch->ColumnAt(j).CopyFiltered(banned));
        }
        return new_batch;
    }
private:
    std::shared_ptr<FilterOperator> filter_operator_;
    std::shared_ptr<PipelineExecutor> child_executor_;
};

class AggregateExecutor : public PipelineExecutor {
public:
    AggregateExecutor(std::shared_ptr<AggregateOperator> aggregation_operator) : aggregation_operator_(aggregation_operator) {
        child_executor_ = ExecuteOperator(aggregation_operator->child);
        if (!child_executor_) {
            throw std::runtime_error("child executor is not set");
        }
    }

    std::shared_ptr<Batch> NextBatch() override {
        if (was_produced) {
            return nullptr;
        }
        was_produced = true;
        while (auto batch = child_executor_->NextBatch()) {
            for (auto& aggr: aggregation_operator_->aggs) {
                aggr->RunBatch(batch);
            }
        }
        auto new_batch = std::make_shared<Batch>(1);
        for (const auto& aggr: aggregation_operator_->aggs) {
            new_batch->AddColumn(aggr->GetResult());
        }
        return new_batch;
    }
private:
    bool was_produced = false;
    std::shared_ptr<AggregateOperator> aggregation_operator_;
    std::shared_ptr<PipelineExecutor> child_executor_;
};

std::shared_ptr<PipelineExecutor> ExecuteOperator(std::shared_ptr<Operator> op) {
    switch (op->type) {
        case OperatorType::SCAN:
            return std::make_shared<ScanExecutor>(std::dynamic_pointer_cast<ScanOperator>(op));
        // case OperatorType::COUNT:
        //     return std::make_shared<CountExecutor>(std::dynamic_pointer_cast<CountOperator>(op));
        case OperatorType::FILTER:
            return std::make_shared<FilterExecutor>(std::dynamic_pointer_cast<FilterOperator>(op));
        case OperatorType::AGGREGATION:
            return std::make_shared<AggregateExecutor>(std::dynamic_pointer_cast<AggregateOperator>(op));
        default:
            throw std::runtime_error("unsupported operator type");
    }
}
