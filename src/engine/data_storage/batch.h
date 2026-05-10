#pragma once

#include <cstddef>
#include <memory>
#include <vector>
#include "engine/data_storage/column.h"
#include "engine/data_storage/schema.h"

using Row = std::vector<std::string>;

class Batch {
public:
    Batch(const Schema& schema, size_t batch_rows_count = Constants::BATCH_SIZE);
    explicit Batch(size_t batch_rows_count);
    void AddRow(Row&& row);
    Row GetRow(size_t row_index) const;
    void AddColumn(size_t column_index, std::vector<std::string>&& values);
    void AddColumn(size_t column_index, std::shared_ptr<Column> column);
    void AddColumn(std::shared_ptr<Column> column);
    void AppendColumn(const std::string& name, Type type, std::shared_ptr<Column> column);
    Column& ColumnAt(size_t column_index);
    const Column& ColumnAt(size_t column_index) const;
    const Schema& GetSchema() const;
    bool HasSchema() const;
    void SetRowsCount(size_t row_count);
    size_t RowsCount() const;
    size_t ColumnsCount() const;
    size_t MaxRowsCount() const;
    bool Empty() const;

private:
    Schema schema;
    std::vector<std::shared_ptr<Column>> columns;
    size_t batch_rows_count;
    size_t rows_count = 0;
    bool has_schema = false;
};
