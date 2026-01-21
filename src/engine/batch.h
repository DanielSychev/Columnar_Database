#pragma once

#include <vector>
#include "engine/column.h"
#include "CSV_reader/reader.h"
#include "engine/schema.h"

struct Batch {
    Batch(Schema& schema);
    bool ReadBatch(Reader& r, size_t rows_cnt); // bool если ничего не прочитали
    void Print(Writer& writer);

    Schema& schema;
    std::vector<std::shared_ptr<Column>> columns;
};