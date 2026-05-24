#include "queries_executor/executor.h"
#include "engine/data_storage/batch.h"
#include "engine/data_storage/schema.h"
#include "queries_executor/helpers.h"
#include "queries_executor/operator.h"
#include "queries_executor/transform.h"
#include <algorithm>
#include <cstddef>
#include <unordered_map>
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
        ReadSchema();
        ReadBatchMetaPositions();
        BuildColumnPositionsAndStarts();
    }

    std::shared_ptr<Batch> NextBatch() override {
        if (next_batch_index >= batch_count) {
            return nullptr;
        }
        data_reader->SetPos(batch_meta_positions[next_batch_index]);
        for (size_t column_index = 0; column_index < column_starts.size(); ++column_index) {
            data_reader->BinaryRead(column_starts[column_index]);
        }

        auto batch = std::make_shared<Batch>(query_schema);
        for (size_t i = 0; i < query_schema.NumColumns(); ++i) {
            data_reader->SetPos(column_starts[column_positions[i]]);
            Column& column = batch->ColumnAt(i);
            column.ReadMf(*data_reader);
            batch->SetRowsCount(column.Size());
        }
        ++next_batch_index;
        return batch;
    }

private:
    void ReadSchema() {
        size_t pos = data_reader->ReadLastBytes(); // позиция меты
        data_reader->SetPos(pos); // читаем схему
        size_t column_count = 0;
        data_reader->BinaryRead(column_count);
        basic_schema.ReadSchema(*data_reader, column_count);
    }

    void ReadBatchMetaPositions() {
        data_reader->BinaryRead(batch_count);
        batch_meta_positions.resize(batch_count);
        for (size_t i = 0; i < batch_meta_positions.size(); ++i) {
            data_reader->BinaryRead(batch_meta_positions[i]);
        }
    }

    void BuildColumnPositionsAndStarts() {
        for (const auto& column_name: scan_operator_->column_names) {
            const auto [type, column_position] =
                queries_executor_detail::ResolveColumn(basic_schema, column_name, "ScanExecutor");
            query_schema.AddColumn(column_name, type);
            column_positions.push_back(column_position);
        }
        column_starts.resize(basic_schema.NumColumns());
    }

    std::shared_ptr<ScanOperator> scan_operator_;
    Reader* data_reader = nullptr;
    Schema basic_schema, query_schema;
    std::vector<size_t> column_positions, column_starts;
    size_t batch_count, next_batch_index = 0;
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
        BuildBanned(batch);
        return batch;
    }
private:
    void BuildBanned(std::shared_ptr<Batch>& batch) {
        std::vector<bool>& banned = batch->banned_rows;
        if (banned.empty()) {
            banned.assign(batch->RowsCount(), false);
        }
        for (size_t i = 0; i < filter_operator_->column_names.size(); ++i) {
            const auto [column_type, column_index] = queries_executor_detail::ResolveColumn(
                batch->GetSchema(),
                filter_operator_->column_names[i],
                "FilterExecutor"
            );
            (void)column_type;
            for (size_t j = 0; j < batch->RowsCount(); ++j) {
                if (banned[j]) {
                    continue;
                }
                if (!batch->ColumnAt(column_index).Compare(filter_operator_->values[i], j, filter_operator_->signs[i])) {
                    banned[j] = true;
                }
            }
        }
    }

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
            batch->AppendColumn(transform->GetResultName(), result_type, transform->Apply(*batch));
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
            if (batch->HasMask()) {
                for (size_t row_index = 0; row_index < batch->RowsCount(); ++row_index) {
                    if (batch->banned_rows[row_index]) continue;
                    for (auto& aggr: aggregation_operator_->aggs) aggr->RunRow(batch, row_index);
                }
            } else {
                for (auto& aggr: aggregation_operator_->aggs) aggr->RunBatch(batch);
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

        bool column_init = false;
        while (auto batch = child_executor_->NextBatch()) {
            if (!column_init) {
                InitializeGroupByColumns(batch);
                column_init = true;
            }
            RunGroupAggregations(batch);
        }
        return BuildResultBatch();
    }
private:
    struct GroupKey {
        std::vector<std::string> values;
        // bool operator==(const GroupKey& other) const {
        //     return values == other.values;
        // }
    };

    struct GroupKeyView {
        std::vector<std::string_view> values;
    };

    struct GroupKeyHash {
        using is_transparent = void;

        static size_t CountHash(const auto& values) {
            size_t seed = 0;
            for (const auto& s : values) {
                seed ^= std::hash<std::string_view>{}(s) + 0x9e3779b9 + (seed << 6) + (seed >> 2); // тут мне помогли, ладно
            }
            return seed;
        }

        size_t operator()(const GroupKey& key) const { return CountHash(key.values); }
        size_t operator()(const GroupKeyView& key) const { return CountHash(key.values); }
    };

    struct GroupKeyEqual {
        using is_transparent = void;

        bool operator()(const GroupKey& a, const GroupKey& b) const { return a.values == b.values; }
        bool operator()(const GroupKeyView& a, const GroupKey& b) const {
            if (a.values.size() != b.values.size()) return false;
            for (size_t i = 0; i < a.values.size(); ++i) {
                if (a.values[i] != b.values[i]) return false;
            }
            return true;
        }
        bool operator()(const GroupKey& a, const GroupKeyView& b) const { return (*this)(b, a); }
    };

    void InitializeGroupByColumns(const std::shared_ptr<Batch>& batch) {
        for (const auto& column_name: group_by_operator_->group_by_columns) {
            const auto [type, column_position] = queries_executor_detail::ResolveColumn(
                batch->GetSchema(),
                column_name,
                "GroupByExecutor"
            );
            result_schema.AddColumn(column_name, type);
            group_by_positions.push_back(column_position);
            group_by_is_string.push_back(type == Type::str);
        }
    }

    void RunGroupAggregations(const std::shared_ptr<Batch>& batch) {
        std::vector<std::string> temp_strings;
        temp_strings.reserve(group_by_positions.size());
        GroupKeyView view_key;
        view_key.values.reserve(group_by_positions.size());
        for (size_t row_index = 0; row_index < batch->RowsCount(); ++row_index) {
            if (batch->HasMask() && batch->banned_rows[row_index]) {
                continue;
            }
            temp_strings.clear();
            view_key.values.clear();
            
            for (size_t i = 0; i < group_by_positions.size(); ++i) {
                if (group_by_is_string[i]) {
                    const auto& str_col = static_cast<const StrColumn&>(batch->ColumnAt(group_by_positions[i]));
                    view_key.values.push_back(str_col.Data()[row_index]);
                } else {
                    temp_strings.push_back(batch->ColumnAt(group_by_positions[i]).GetElemToString(row_index));
                    view_key.values.push_back(temp_strings.back());
                }
            }

            // GroupKey current_group_key;
            // for (auto& column_index: group_by_positions) {
            //     current_group_key.values.push_back(batch->ColumnAt(column_index).GetElemToString(row_index));
            // }
            // auto& aggs = groups_aggs[current_group_key];
            // if (aggs.empty()) {
            //     for (const auto& aggr: group_by_operator_->aggs) {
            //         aggs.push_back(aggr->Clone());
            //     }
            // }
            
            auto it = groups_aggs.find(view_key);
            if (it == groups_aggs.end()) {
                GroupKey owning_key;
                for (const auto& sv : view_key.values) {
                    owning_key.values.emplace_back(sv);
                }
                it = groups_aggs.emplace(std::move(owning_key), std::vector<std::shared_ptr<Aggregation>>{}).first;
                for (const auto& aggr: group_by_operator_->aggs) {
                    it->second.push_back(aggr->Clone());
                }
            }

            for (auto& aggr: it->second) {
                aggr->RunRow(batch, row_index);
            }
        }
    }

    std::shared_ptr<Batch> BuildResultBatch() {
        std::shared_ptr<Batch> result_batch;
        for (auto& [group_key, aggs]: groups_aggs) {
            std::vector<std::string> result_values;
            for (const auto& value: group_key.values) {
                result_values.push_back(value);
            }
            for (const auto& aggr: aggs) {
                if (result_schema.NumColumns() < group_by_positions.size() + aggs.size()) {
                    result_schema.AddColumn(aggr->result_name, aggr->GetResultType());
                }
                result_values.push_back(aggr->GetResultValue());
            }
            if (!result_batch) {
                result_batch = std::make_shared<Batch>(result_schema, groups_aggs.size());
            }
            result_batch->AddRow(std::move(result_values));
        }
        return result_batch;
    }

    std::shared_ptr<GroupByOperator> group_by_operator_;
    std::shared_ptr<PipelineExecutor> child_executor_;
    std::vector<bool> group_by_is_string;
    std::unordered_map<GroupKey, std::vector<std::shared_ptr<Aggregation>>, GroupKeyHash, GroupKeyEqual> groups_aggs;
    bool was_produced = false;
    Schema result_schema;
    std::vector<size_t> group_by_positions;
    // std::unordered_map<GroupKey, std::vector<std::shared_ptr<Aggregation>>, GroupKeyHash> groups_aggs;
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
        FindOrderColumns(batch);
        auto heap = ConsumeBatchesIntoHeap(batch);
        BuildSortedRows(heap);
        return BuildResultBatch();
    }

private:
    bool ComesBefore(const Row& left, const Row& right) const {
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

    struct RowComparator {
        const OrderByExecutor* executor;

        bool operator()(const Row& left, const Row& right) const {
            return executor->ComesBefore(left, right);
        }
    };

    using RowHeap = std::priority_queue<Row, std::vector<Row>, RowComparator>;

    void FindOrderColumns(const std::shared_ptr<Batch>& batch) {
        descending = order_by_operator_->descending;
        schema = batch->GetSchema();
        for (auto& column_name: order_by_operator_->column_names) {
            const auto [type, column_position] =
                queries_executor_detail::ResolveColumn(schema, column_name, "OrderByExecutor");
            (void)type;
            column_indices.push_back(column_position);
        }
    }

    RowHeap ConsumeBatchesIntoHeap(std::shared_ptr<Batch> batch) {
        RowHeap heap(RowComparator{this});
        const size_t limit = order_by_operator_->limit;
        const size_t offset = order_by_operator_->offset;
        if (limit == 0) {
            return heap;
        }

        const size_t max_heap_size = (limit > SIZE_MAX - offset) ? SIZE_MAX : limit + offset;

        auto consume_batch = [&](const std::shared_ptr<Batch>& batch) {
            for (size_t row_index = 0; row_index < batch->RowsCount(); ++row_index) {
                if (batch->HasMask() && batch->banned_rows[row_index]) {
                    continue;
                }
                Row row = batch->GetRow(row_index);

                if (heap.size() < max_heap_size) {
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
        return heap;
    }

    void BuildSortedRows(RowHeap& heap) {
        const size_t limit = order_by_operator_->limit;
        const size_t offset = order_by_operator_->offset;

        if (heap.size() <= offset) {
            return;
        }

        size_t rows_to_take = std::min(limit, heap.size() - offset);

        while (!heap.empty() && rows_to_take > 0) {
            sorted_rows.push_back(heap.top());
            heap.pop();
            --rows_to_take;
        }
        std::reverse(sorted_rows.begin(), sorted_rows.end());
    }

    std::shared_ptr<Batch> BuildResultBatch() {
        auto result_batch = std::make_shared<Batch>(schema, sorted_rows.size());
        for (auto& row: sorted_rows) {
            result_batch->AddRow(std::move(row));
        }
        return result_batch;
    }

    std::shared_ptr<OrderByOperator> order_by_operator_;
    std::shared_ptr<PipelineExecutor> child_executor_;
    std::vector<size_t> column_indices;
    std::vector<Row> sorted_rows;
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
