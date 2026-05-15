#pragma once

#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"
#include "engine/data_storage/batch.h"

namespace batch_serialization {
bool ReadCsvBatch(Reader& reader, Batch& batch);
bool ReadMfBatch(Reader& reader, Batch& batch);
size_t WriteMfBatch(const Batch& batch, Writer& writer); // будет возвращать meta_position
void WriteCsvBatch(const Batch& batch, Writer& writer);
}
