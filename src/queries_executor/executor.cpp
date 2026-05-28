#include "queries_executor/executor.h"
#include "engine/data_storage/batch.h"
#include "engine/data_storage/schema.h"
#include "queries_executor/aggregation.h"
#include "queries_executor/helpers.h"
#include "queries_executor/operator.h"
#include "queries_executor/transform.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <queue>
#include <stdexcept>
#include <string>
#include <string_view>
#include <boost/unordered/unordered_flat_map.hpp>

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
            batch->ColumnAt(column_index).Filter(filter_operator_->values[i], filter_operator_->signs[i], banned);
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
            const size_t n = batch->RowsCount();
            std::vector<size_t> group_indices(n, 0);
            if (batch->HasMask()) {
                for (size_t i = 0; i < n; ++i) {
                    if (batch->banned_rows[i]) group_indices[i] = SIZE_MAX;
                }
            }
            for (auto& aggr: aggregation_operator_->aggs) {
                aggr->RunBatch(batch, group_indices);
            }
        }
        Schema result_schema;
        std::vector<std::string> result_values;
        for (const auto& aggr: aggregation_operator_->aggs) {
            result_schema.AddColumn(aggr->result_name, aggr->GetResultType());
            result_values.push_back(aggr->GetResultValue(0));
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
    GroupByExecutor(std::shared_ptr<GroupByOperator> group_by_operator) : group_by_operator_(group_by_operator), aggs(group_by_operator->aggs) {
        child_executor_ = CreateChildExecutor(group_by_operator_->child, "GroupByExecutor");
    }

    std::shared_ptr<Batch> NextBatch() override {
        if (was_produced) {
            return nullptr;
        }
        was_produced = true;

        auto batch = child_executor_->NextBatch();
        if (!batch) {
            return BuildResultBatch();
        }
        InitializeGroupByColumns(batch);
        do {
            RunGroupAggregations(batch);
            batch = child_executor_->NextBatch();
        } while (batch);
        return BuildResultBatch();
    }
private:
    struct GroupKey {
        uint32_t offset;
        uint32_t length;
    };

    struct GroupKeyHash {
        using is_transparent = void;
        const std::vector<char>* buf;

        size_t operator()(std::string_view k) const noexcept {
            return std::hash<std::string_view>{}(k);
        }
        size_t operator()(GroupKey k) const noexcept {
            return std::hash<std::string_view>{}({buf->data() + k.offset, k.length});
        }
    };

    struct GroupKeyEqual {
        using is_transparent = void;
        const std::vector<char>* buf;

        bool operator()(GroupKey a, GroupKey b) const noexcept {
            return a.length == b.length && std::memcmp(buf->data() + a.offset, buf->data() + b.offset, a.length) == 0;
        }
        bool operator()(std::string_view a, GroupKey b) const noexcept {
            return a.size() == b.length && std::memcmp(a.data(), buf->data() + b.offset, a.size()) == 0;
        }
        bool operator()(GroupKey a, std::string_view b) const noexcept { return (*this)(b, a); }
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
        }
    }

    void RunGroupAggregations(const std::shared_ptr<Batch>& batch) {
        const size_t rows_count = batch->RowsCount();
        group_indices.assign(rows_count, SIZE_MAX);
        for (size_t row_index = 0; row_index < rows_count; ++row_index) {
            if (batch->HasMask() && batch->banned_rows[row_index]) {
                continue;
            }
            temp_key_buf.clear();
            for (size_t column_position : group_by_positions) {
                batch->ColumnAt(column_position).BinaryWriteInBuf(temp_key_buf, row_index);
            }

            std::string_view view_key{temp_key_buf.data(), temp_key_buf.size()};
            auto it = groups_aggs.find(view_key);
            if (it == groups_aggs.end()) {
                const uint32_t offset = static_cast<uint32_t>(keys_buf.size());
                keys_buf.insert(keys_buf.end(), view_key.data(), view_key.data() + view_key.size());
                it = groups_aggs.emplace(GroupKey{offset, static_cast<uint32_t>(view_key.size())}, groups_count++).first;
            }
            group_indices[row_index] = it->second;
        }
        for (auto& aggr : aggs) {
            aggr->RunBatch(batch, group_indices);
        }
    }

    std::shared_ptr<Batch> BuildResultBatch() {
        for (const auto& aggr: aggs) {
            result_schema.AddColumn(aggr->result_name, aggr->GetResultType());
        }
        auto result_batch = std::make_shared<Batch>(result_schema, groups_aggs.size());
        for (auto& [group_key, group_ind]: groups_aggs) {
            const char* key_ptr = keys_buf.data() + group_key.offset;
            for (size_t i = 0; i < group_by_positions.size(); ++i) {
                result_batch->ColumnAt(i).BinaryReadFromBuf(key_ptr);
            }
            for (size_t i = 0; i < aggs.size(); ++i) {
                const auto& aggr = aggs[i];
                aggr->GetResultInto(result_batch->ColumnAt(group_by_positions.size() + i), group_ind);
            }
        }
        result_batch->SetRowsCount(groups_aggs.size());
        return result_batch;
    }

    std::shared_ptr<GroupByOperator> group_by_operator_;
    std::shared_ptr<PipelineExecutor> child_executor_;
    bool was_produced = false;
    Schema result_schema;
    std::vector<size_t> group_by_positions;
    size_t groups_count = 0;
    std::vector<std::shared_ptr<Aggregation>>& aggs;


    std::vector<char> keys_buf;
    std::vector<char> temp_key_buf;
    std::vector<size_t> group_indices;
    boost::unordered_flat_map<GroupKey, size_t, GroupKeyHash, GroupKeyEqual> groups_aggs{16, GroupKeyHash{&keys_buf}, GroupKeyEqual{&keys_buf}};
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

    struct RowRef {
        std::shared_ptr<Batch> batch;
        uint32_t row_idx;
    };

    bool ComesBefore(const RowRef& left, const RowRef& right) const {
        for (size_t col : column_indices) {
            int cmp = left.batch->ColumnAt(col).CompareAt(
                left.row_idx, right.batch->ColumnAt(col), right.row_idx);
            if (cmp != 0) return descending ? cmp > 0 : cmp < 0;
        }
        return false;
    }

    struct RowComparator {
        const OrderByExecutor* executor;

        bool operator()(const RowRef& left, const RowRef& right) const {
            return executor->ComesBefore(left, right);
        }
    };

    using RowHeap = std::priority_queue<RowRef, std::vector<RowRef>, RowComparator>;

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
                RowRef row{batch, static_cast<uint32_t>(row_index)};

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
            for (size_t c = 0; c < schema.NumColumns(); ++c) {
                result_batch->ColumnAt(c).AppendFrom(row.batch->ColumnAt(c), row.row_idx);
            }
        }
        result_batch->SetRowsCount(sorted_rows.size());
        return result_batch;
    }

    std::shared_ptr<OrderByOperator> order_by_operator_;
    std::shared_ptr<PipelineExecutor> child_executor_;
    std::vector<size_t> column_indices;
    std::vector<RowRef> sorted_rows;
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
        if (rows_num == batch->RowsCount()) {
            return batch;
        }
        std::vector<bool>& banned = batch->banned_rows;
        if (banned.empty()) {
            banned.assign(batch->RowsCount(), false);
        }
        for (size_t i = rows_num; i < batch->RowsCount(); ++i) {
            banned[i] = true;
        }
        return batch;
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
