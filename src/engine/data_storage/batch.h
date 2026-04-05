#pragma once

#include <cstddef>
#include <memory>
#include <vector>
#include "engine/data_storage/column.h"
#include "engine/data_storage/schema.h"

class Batch {
public:
    Batch(Schema& schema, size_t batch_rows_count);
    void AddRow(std::vector<std::string>&& row);
    void AddColumn(size_t column_index, std::vector<std::string>&& values);
    Column& ColumnAt(size_t column_index);
    const Column& ColumnAt(size_t column_index) const;
    void SetRowsCount(size_t row_count);
    size_t RowsCount() const;
    size_t ColumnsCount() const;
    size_t MaxRowsCount() const;
    bool Empty() const;

private:
    std::vector<std::shared_ptr<Column>> columns;
    size_t batch_rows_count;
    size_t rows_count = 0;
};
