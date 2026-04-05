#include "engine/data_storage/batch.h"
#include <cstddef>
#include <memory>
#include <stdexcept>

namespace {
std::shared_ptr<Column> CreateColumn(Type type) {
    switch (type) {
    case Type::int64:
        return std::make_shared<Int64Column>();
    case Type::str:
        return std::make_shared<StrColumn>();
    default:
        return std::make_shared<StrColumn>();
    }
}
}

Batch::Batch(Schema& schema, size_t batch_rows_count) : batch_rows_count(batch_rows_count) {
    columns.resize(schema.NumColums());
    for (size_t i = 0; i < columns.size(); ++i) {
        columns[i] = CreateColumn(schema.types[i]);
    }
}

void Batch::AddRow(std::vector<std::string>&& row) {
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

void Batch::AddColumn(size_t column_index, std::vector<std::string>&& values) {
    if (column_index >= columns.size()) {
        throw std::runtime_error("wrong column index");
    }
    if (columns[column_index]->Size() != 0) {
        throw std::runtime_error("column is already filled");
    }
    if (values.size() > batch_rows_count) {
        throw std::runtime_error("wrong batch format");
    }

    SetRowsCount(values.size());

    for (auto& value : values) {
        columns[column_index]->AddElem(std::move(value));
    }
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
