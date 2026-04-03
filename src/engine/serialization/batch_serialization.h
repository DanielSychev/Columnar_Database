#pragma once

#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"
#include "engine/data_storage/batch.h"

namespace batch_serialization {
bool ReadCsvBatch(Reader& reader, Batch& batch);
bool ReadMfBatch(Reader& reader, Batch& batch);
void WriteMfBatch(const Batch& batch, Writer& writer);
void WriteCsvBatch(const Batch& batch, Writer& writer);
}
