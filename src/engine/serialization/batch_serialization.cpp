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
        batch.ColumnAt(i).Read(reader);
        batch.SetRowsCount(batch.ColumnAt(i).Size());
    }
    return !batch.Empty();
}

void WriteMfBatch(const Batch& batch, Writer& writer) {
    for (size_t i = 0; i < batch.ColumnsCount(); ++i) {
        batch.ColumnAt(i).Print(writer);
    }
}

void WriteCsvBatch(const Batch& batch, Writer& writer) {
    for (size_t row_index = 0; row_index < batch.RowsCount(); ++row_index) {
        for (size_t column_index = 0; column_index < batch.ColumnsCount(); ++column_index) {
            batch.ColumnAt(column_index).PrintElem(writer, row_index, column_index == batch.ColumnsCount() - 1);
        }
    }
}
}
