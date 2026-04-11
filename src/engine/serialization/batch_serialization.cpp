#include "engine/serialization/batch_serialization.h"
#include <stdexcept>
#include <vector>

namespace batch_serialization {
bool ReadCsvBatch(Reader& reader, Batch& batch) {
    std::vector<std::vector<std::string>> rows(batch.MaxRowsCount());
    if (!reader.ReadRows(rows, batch.MaxRowsCount())) {
        return false;
    }

    for (auto& row : rows) {
        if (row.empty()) {
            break;
        }
        batch.AddRow(std::move(row));
    }
    return !batch.Empty();
}

bool ReadMfBatch(Reader& reader, Batch& batch) {
    for (size_t i = 0; i < batch.ColumnsCount(); ++i) {
        Column& column = batch.ColumnAt(i);
        column.Read(reader);
        batch.SetRowsCount(column.Size());
    }
    return !batch.Empty();
}

size_t WriteMfBatch(const Batch& batch, Writer& writer) {
    size_t columns_count = batch.ColumnsCount();
    std::vector<size_t> column_starts(columns_count);
    for (size_t i = 0; i < columns_count; ++i) {
        column_starts[i] = writer.TellPos();
        batch.ColumnAt(i).Print(writer);
    }
    // writing batch meta
    size_t meta_pos = writer.TellPos();
    for (size_t i = 0; i < columns_count; ++i) {
        writer.BinaryWrite(column_starts[i]);
    }
    return meta_pos;
}

void WriteCsvBatch(const Batch& batch, Writer& writer) {
    size_t rows_count = batch.RowsCount();
    size_t columns_count = batch.ColumnsCount();
    for (size_t row_index = 0; row_index < rows_count; ++row_index) {
        for (size_t column_index = 0; column_index < columns_count; ++column_index) {
            batch.ColumnAt(column_index).PrintElem(writer, row_index, column_index == columns_count - 1);
        }
    }
}
}
