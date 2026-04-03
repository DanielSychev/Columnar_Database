#include "engine/data_storage/batch.h"
#include <cstddef>
#include <memory>

Batch::Batch(Schema& schema, size_t batch_rows_count) : schema(schema), batch_rows_count(batch_rows_count) {
    columns.resize(schema.NumColums());
    for (size_t i = 0; i < columns.size(); ++i) {
        switch (schema.types[i]) {
        case Type::int64:
            columns[i] = std::make_shared<Int64Column>();
            break;
        case Type::str:
            columns[i] = std::make_shared<StrColumn>();
            break;
        default:
            columns[i] = std::make_shared<StrColumn>();
            break;
            // throw std::runtime_error("bad column type was given");
        }
    }
}

bool Batch::CSVReadBatch(Reader& r) {
    std::vector<std::vector<std::string>> rows(batch_rows_count);
    if (!r.ReadRows(rows, batch_rows_count)) {
        return false;
    }
    
    for (auto& row : rows) {
        if (row.empty()) { // конец файла
            break;
        }
        if (row.size() != columns.size()) {
            throw std::runtime_error("wrong schema formart / wrong row lenght");
        }
        for (size_t i = 0; i < row.size(); ++i) {
            columns[i]->AddElem(std::move(row[i]));
        }
    }
    return true;
}

bool Batch::MFReadBatch(Reader& r) {
    std::vector<std::vector<std::string>> mfcolumns(schema.NumColums());
    if (!r.ReadRows(mfcolumns, schema.NumColums())) {
        return false;
    }
    for (size_t i = 0; i < mfcolumns.size(); ++i) {
        if (mfcolumns[i].empty()) { // неполный батч (последний который)
            break;
        }
        if (mfcolumns[i].size() > batch_rows_count) {
            throw std::runtime_error("wrong batch format");
        }
        for (auto& elem : mfcolumns[i]) {
            columns[i]->AddElem(std::move(elem));
        }
    }
    return true;
}

void Batch::MFPrintBatch(Writer& writer) {
    for (auto& column : columns) {
        column -> Print(writer);
    }
}

void Batch::CSVPrintBatch(Writer& writer) {
    for (size_t j = 0; j < batch_rows_count; ++j) {
        for (size_t i = 0; i < columns.size(); ++i) {
            columns[i]->PrintElem(writer, j, i == columns.size() - 1);
        }
    }
}