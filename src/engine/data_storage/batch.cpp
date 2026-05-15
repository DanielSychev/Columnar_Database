#include "engine/data_storage/batch.h"
#include "engine/data_storage/schema.h"
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <vector>

Batch::Batch(const Schema& schema, size_t batch_rows_count) : schema(schema), batch_rows_count(batch_rows_count) {
    has_schema = true;
    columns.resize(schema.NumColumns());
    for (size_t i = 0; i < columns.size(); ++i) {
        columns[i] = CreateColumn(this->schema.ColumnTypeAt(i));
    }
}

Batch::Batch(size_t batch_rows_count) : batch_rows_count(batch_rows_count) {
}

void Batch::AddRow(Row&& row) {
    if (!has_schema) {
        throw std::runtime_error("cannot add rows to batch without schema");
    }
    if (row.size() != columns.size()) {
        throw std::runtime_error("wrong schema formart / wrong row lenght");
    }
    if (rows_count >= batch_rows_count) {
        throw std::runtime_error("batch is full");
    }
    for (size_t i = 0; i < row.size(); ++i) {
        columns[i]->AddElem(std::move(row[i]));
    }
    ++rows_count;
}

Row Batch::GetRow(size_t row_index) const {
    if (row_index >= rows_count) {
        throw std::runtime_error("wrong row index");
    }
    Row row;
    row.reserve(columns.size());
    for (const auto& column: columns) {
        row.push_back(column->GetElemToString(row_index));
    }
    return row;
}

void Batch::AddColumn(size_t column_index, std::vector<std::string>&& values) {
    ValidateColumnIndex(column_index, values.size());

    SetRowsCount(values.size());
    columns[column_index] = CreateColumn(schema.ColumnTypeAt(column_index), values);
}

void Batch::AddColumn(size_t column_index, std::shared_ptr<Column> column) {
    if (!column) {
        throw std::runtime_error("column is null");
    }
    ValidateColumnIndex(column_index, column->Size());

    SetRowsCount(column->Size());
    columns[column_index] = std::move(column);
}

void Batch::AddColumn(std::shared_ptr<Column> column) {
    if (has_schema) {
        throw std::runtime_error("cannot append column to batch with schema");
    }
    if (!column) {
        throw std::runtime_error("column is null");
    }

    SetRowsCount(column->Size());
    columns.push_back(std::move(column));
}

void Batch::AppendColumn(const std::string& name, Type type, std::shared_ptr<Column> column) {
    if (!has_schema) {
        throw std::runtime_error("cannot append named column to batch without schema");
    }
    if (!column) {
        throw std::runtime_error("column is null");
    }

    SetRowsCount(column->Size());
    schema.AddColumn(name, type);
    columns.push_back(std::move(column));
}

Column& Batch::ColumnAt(size_t column_index) {
    if (column_index >= columns.size()) {
        throw std::runtime_error("wrong column index");
    }
    return *columns[column_index];
}

const Column& Batch::ColumnAt(size_t column_index) const {
    if (column_index >= columns.size()) {
        throw std::runtime_error("wrong column index");
    }
    return *columns[column_index];
}

const Schema& Batch::GetSchema() const {
    if (!has_schema) {
        throw std::runtime_error("batch has no schema");
    }
    return schema;
}

bool Batch::HasSchema() const {
    return has_schema;
}

void Batch::SetRowsCount(size_t row_count) {
    if (row_count > batch_rows_count) {
        throw std::runtime_error("wrong batch format");
    }
    if (rows_count == 0) {
        rows_count = row_count;
        return;
    }
    if (rows_count != row_count) {
        throw std::runtime_error("wrong batch format");
    }
}

size_t Batch::RowsCount() const {
    return rows_count;
}

size_t Batch::ColumnsCount() const {
    return columns.size();
}

size_t Batch::MaxRowsCount() const {
    return batch_rows_count;
}

bool Batch::Empty() const {
    return rows_count == 0;
}

void Batch::ValidateColumnIndex(size_t column_index, size_t row_count) const {
    if (!has_schema) {
        throw std::runtime_error("cannot add indexed column to batch without schema");
    }
    if (column_index >= columns.size()) {
        throw std::runtime_error("wrong column index");
    }
    if (columns[column_index]->Size() != 0) {
        throw std::runtime_error("column is already filled");
    }
    if (row_count > batch_rows_count) {
        throw std::runtime_error("too many rows for batch");
    }
}
