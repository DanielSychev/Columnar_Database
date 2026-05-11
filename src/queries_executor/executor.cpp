#include "queries_executor/executor.h"
#include "engine/data_storage/schema.h"
#include "queries_executor/helpers.h"
#include "queries_executor/operator.h"
#include "queries_executor/transform.h"
#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <numeric>
#include <queue>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {
std::shared_ptr<PipelineExecutor> CreateChildExecutor(const std::shared_ptr<Operator>& child, std::string_view context) {
    auto child_executor = ExecuteOperator(child);
    if (!child_executor) {
        throw std::runtime_error("child executor is not set (in " + std::string(context) + ")");
    }
    return child_executor;
}
}

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
            const auto [type, column_position] =
                queries_executor_detail::ResolveColumn(basic_schema, column_name, "ScanExecutor");
            query_schema.AddColumn(column_name, type);
            column_positions.push_back(column_position);
        }
        column_starts.resize(basic_schema.NumColumns());
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
        for (size_t i = 0; i < query_schema.NumColumns(); ++i) {
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

class FilterExecutor : public PipelineExecutor {
public:
    FilterExecutor(std::shared_ptr<FilterOperator> filter_op) : filter_operator_(filter_op) {
        child_executor_ = CreateChildExecutor(filter_op->child, "FilterExecutor");
    }

    std::shared_ptr<Batch> NextBatch() override {
        auto batch = child_executor_->NextBatch();
        if (!batch) {
            return nullptr;
        }
        std::vector<bool> banned(batch->RowsCount(), false);
        for (size_t i = 0; i < filter_operator_->column_names.size(); ++i) {
            const auto [column_type, column_index] = queries_executor_detail::ResolveColumn(
                batch->GetSchema(),
                filter_operator_->column_names[i],
                "FilterExecutor"
            );
            (void)column_type;
            for (size_t j = 0; j < batch->RowsCount(); ++j) {
                if (!batch->ColumnAt(column_index).Compare(filter_operator_->values[i], j, filter_operator_->signs[i])) {
                    banned[j] = true;
                }
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

class TransformExecutor : public PipelineExecutor {
public:
    TransformExecutor(std::shared_ptr<TransformsOperator> transform_operator) : transform_operator_(std::move(transform_operator)) {
        child_executor_ = CreateChildExecutor(transform_operator_->child, "TransformExecutor");
    }

    std::shared_ptr<Batch> NextBatch() override {
        auto batch = child_executor_->NextBatch();
        if (!batch || transform_operator_->transforms.empty()) {
            return batch;
        }

        for (const auto& transform : transform_operator_->transforms) {
            const Type result_type = transform->ResultType(batch->GetSchema());
            batch->AppendColumn(transform->result_name, result_type, transform->Apply(*batch));
        }

        return batch;
    }

private:
    std::shared_ptr<TransformsOperator> transform_operator_;
    std::shared_ptr<PipelineExecutor> child_executor_;
};

class AggregateExecutor : public PipelineExecutor {
public:
    AggregateExecutor(std::shared_ptr<AggregateOperator> aggregation_operator) : aggregation_operator_(aggregation_operator) {
        child_executor_ = CreateChildExecutor(aggregation_operator->child, "AggregateExecutor");
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
        Schema result_schema;
        std::vector<std::string> result_values;
        for (const auto& aggr: aggregation_operator_->aggs) {
            result_schema.AddColumn(aggr->result_name, aggr->GetResultType());
            result_values.push_back(aggr->GetResultValue());
        }
        auto result_batch = std::make_shared<Batch>(result_schema, 1);
        result_batch->AddRow(std::move(result_values));
        return result_batch;
    }
private:
    bool was_produced = false;
    std::shared_ptr<AggregateOperator> aggregation_operator_;
    std::shared_ptr<PipelineExecutor> child_executor_;
};

class GroupByExecutor : public PipelineExecutor {
public:
    GroupByExecutor(std::shared_ptr<GroupByOperator> group_by_operator) : group_by_operator_(group_by_operator) {
        child_executor_ = CreateChildExecutor(group_by_operator_->child, "GroupByExecutor");
    }

    std::shared_ptr<Batch> NextBatch() override {
        if (was_produced) {
            return nullptr;
        }
        was_produced = true;

        bool filled_schema = false;
        while (auto batch = child_executor_->NextBatch()) {
            if (!filled_schema) {
                for (const auto& column_name: group_by_operator_->group_by_columns) {
                    const auto [type, column_position] = queries_executor_detail::ResolveColumn(
                        batch->GetSchema(),
                        column_name,
                        "GroupByExecutor"
                    );
                    result_schema.AddColumn(column_name, type);
                    group_by_positions.push_back(column_position);
                }
                filled_schema = true;
            }
            
            std::map<GroupKey, std::shared_ptr<Batch>> group_batches;
            for (size_t row_index = 0; row_index < batch->RowsCount(); ++row_index) {
                GroupKey current_group_key;
                for (auto& column_index: group_by_positions) {
                    current_group_key.values.push_back(batch->ColumnAt(column_index).GetElemToString(row_index));
                }
                if (group_batches.find(current_group_key) == group_batches.end()) {
                    group_batches[current_group_key] = std::make_shared<Batch>(batch->GetSchema());
                }
                group_batches[current_group_key]->AddRow(batch->GetRow(row_index)); // копируем всё, а можно только те колонки, которые нужны для аггрегаций, но это сложнее реализовать
            }
            for (auto& [group_key, group_batch]: group_batches) {
                auto& aggs = groups_aggs[group_key];
                if (aggs.empty()) {
                    for (const auto& aggr: group_by_operator_->aggs) {
                        aggs.push_back(aggr->Clone());
                    }
                }
                for (auto& aggr: aggs) {
                    aggr->RunBatch(group_batch);
                }
            }
        }
        filled_schema = false;
        std::shared_ptr<Batch> result_batch;
        for (auto& [group_key, aggs]: groups_aggs) {
            std::vector<std::string> result_values;
            for (const auto& value: group_key.values) {
                result_values.push_back(value);
            }
            for (const auto& aggr: aggs) {
                if (!filled_schema) {
                    result_schema.AddColumn(aggr->result_name, aggr->GetResultType());
                }
                result_values.push_back(aggr->GetResultValue());
            }
            filled_schema = true;
            if (!result_batch) {
                result_batch = std::make_shared<Batch>(result_schema, groups_aggs.size());
            }
            result_batch->AddRow(std::move(result_values));
        }
        return result_batch;
    }
private:
    struct GroupKey {
        std::vector<std::string> values;
        bool operator<(const GroupKey& other) const {
            return values < other.values;
        }
    };


    std::shared_ptr<GroupByOperator> group_by_operator_;
    std::shared_ptr<PipelineExecutor> child_executor_;
    bool was_produced = false;
    Schema result_schema;
    std::vector<size_t> group_by_positions;
    std::map<GroupKey, std::vector<std::shared_ptr<Aggregation>>> groups_aggs;
};

class OrderByExecutor : public PipelineExecutor {
public:

    OrderByExecutor(std::shared_ptr<OrderByOperator> order_by_operator) : order_by_operator_(order_by_operator) {
        child_executor_ = CreateChildExecutor(order_by_operator_->child, "OrderByExecutor");
    }

    std::shared_ptr<Batch> NextBatch() override {
        if (was_produced) {
            return nullptr;
        }
        was_produced = true;

        auto batch = child_executor_->NextBatch();
        if (!batch) {
            return nullptr;
        }
        descending = order_by_operator_->descending;
        schema = batch->GetSchema();
        for (auto& column_name: order_by_operator_->column_names) {
            const auto [type, column_position] =
                queries_executor_detail::ResolveColumn(schema, column_name, "OrderByExecutor");
            (void)type;
            column_indices.push_back(column_position);
        }
        auto cmp = [this](const Row& left, const Row& right) {
            return this->ComesBefore(left, right);
        };
        std::priority_queue<Row, std::vector<Row>, decltype(cmp)> heap(cmp);
        const size_t limit = order_by_operator_->limit;
        auto consume_batch = [&](const std::shared_ptr<Batch>& batch) {
            for (size_t row_index = 0; row_index < batch->RowsCount(); ++row_index) {
                Row row = batch->GetRow(row_index);

                if (heap.size() < limit) {
                    heap.push(std::move(row));
                    continue;
                }

                if (ComesBefore(row, heap.top())) {
                    heap.pop();
                    heap.push(std::move(row));
                }
            }
        };
        do {
            consume_batch(batch);
            batch = child_executor_->NextBatch();
        } while (batch);
        std::vector<Row> sorted_rows;
        while (!heap.empty()) {
            sorted_rows.push_back(std::move(heap.top()));
            heap.pop();
        }
        std::reverse(sorted_rows.begin(), sorted_rows.end());
        auto result_batch = std::make_shared<Batch>(schema, sorted_rows.size());
        for (auto& row: sorted_rows) {
            result_batch->AddRow(std::move(row));
        }
        return result_batch;
    }

private:
    bool ComesBefore(const Row& left, const Row& right) {
        for (auto& i : column_indices) {
            const auto cmp = queries_executor_detail::CompareTypedValues(
                schema.ColumnTypeAt(i),
                left[i],
                right[i],
                "OrderByExecutor"
            );
            if (cmp != 0) {
                return descending ? cmp > 0 : cmp < 0;
            }
        }
        return false;
    }


    std::shared_ptr<OrderByOperator> order_by_operator_;
    std::shared_ptr<PipelineExecutor> child_executor_;
    std::vector<size_t> column_indices;
    bool descending;
    Schema schema;
    bool was_produced = false;
};


class LimitExecutor : public PipelineExecutor {
public:
    LimitExecutor(std::shared_ptr<LimitOperator> limit_operator) : limit_operator_(limit_operator) {
        child_executor_ = CreateChildExecutor(limit_operator_->child, "LimitExecutor");
    }

    std::shared_ptr<Batch> NextBatch() override {
        auto batch = child_executor_->NextBatch();
        if (limit_operator_->limit == 0 || !batch) {
            return nullptr;
        }
        
        size_t rows_num = std::min(limit_operator_->limit, batch->RowsCount());
        limit_operator_->limit -= rows_num;
        auto limit_batch = std::make_shared<Batch>(batch->GetSchema(), rows_num);
        std::vector<size_t> order(rows_num);
        std::iota(order.begin(), order.end(), 0);
        for (size_t i = 0; i < batch->ColumnsCount(); ++i) {
            limit_batch->AddColumn(i, batch->ColumnAt(i).CopyReordered(order));
        }
        return limit_batch;
    }
private:
    std::shared_ptr<LimitOperator> limit_operator_;
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
        case OperatorType::TRANSFORM:
            return std::make_shared<TransformExecutor>(std::dynamic_pointer_cast<TransformsOperator>(op));
        case OperatorType::AGGREGATION:
            return std::make_shared<AggregateExecutor>(std::dynamic_pointer_cast<AggregateOperator>(op));
        case OperatorType::GROUPBY:
            return std::make_shared<GroupByExecutor>(std::dynamic_pointer_cast<GroupByOperator>(op));
        case OperatorType::ORDERBY:
            return std::make_shared<OrderByExecutor>(std::dynamic_pointer_cast<OrderByOperator>(op));
        case OperatorType::LIMIT:
            return std::make_shared<LimitExecutor>(std::dynamic_pointer_cast<LimitOperator>(op));
        default:
            throw std::runtime_error("unsupported operator type");
    }
}
