#include "engine/batch.h"
#include <cstddef>
#include <memory>

Batch::Batch(Schema& schema) : schema(schema) {
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
            throw std::runtime_error("bad column type was given");
        }
    }
}

bool Batch::ReadBatch(Reader& r, size_t rows_cnt) {
    std::vector<std::vector<std::string>> rows(rows_cnt);
    if (!r.ReadRows(rows, rows_cnt)) {
        return false;
    }
    
    for (auto row : rows) {
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

void Batch::Print(Writer& writer) {
    for (auto& column : columns) {
        column -> Print(writer);
    }
}