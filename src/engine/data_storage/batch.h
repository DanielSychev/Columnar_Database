#pragma once

#include <vector>
#include "engine/data_storage/column.h"
#include "CSV_reader/reader.h"
#include "engine/data_storage/schema.h"

struct Batch {
    Batch(Schema& schema, size_t batch_rows_count);
    bool CSVReadBatch(Reader& r); // bool если ничего не прочитали
    bool MFReadBatch(Reader& r); // batch reader from my format
    void MFPrintBatch(Writer& writer);
    void CSVPrintBatch(Writer& writer);

    Schema& schema;
    std::vector<std::shared_ptr<Column>> columns;
    size_t batch_rows_count;
};