#include "engine/engine.h"
#include "engine/serialization/batch_serialization.h"
#include <stdexcept>

MfFileWriter::MfFileWriter(std::ifstream& csv_stream, std::ofstream& mf_stream, std::ifstream& schema_stream)
    : csv_reader(csv_stream), mf_writer(mf_stream), schema_reader(schema_stream) {}

void MfFileWriter::ProcessBatches(const Schema& schema) {
    while (true) {
        Batch batch(schema, batch_rows_count);
        if (!batch_serialization::ReadCsvBatch(csv_reader, batch)) {
            break;
        }
        size_t meta_pos = batch_serialization::WriteMfBatch(batch, mf_writer);
        batch_meta_positions.push_back(meta_pos);
    }
}

void MfFileWriter::Convert() {
    Schema schema;
    schema.ReadSchema(schema_reader);
    if (schema.NumColumns() == 0) {
        throw std::runtime_error("schema is empty or was not read");
    }
    ProcessBatches(schema);

    size_t pos = mf_writer.TellPos();
    mf_writer.BinaryWrite(schema.NumColumns());
    schema.PrintSchema(mf_writer);

    mf_writer.BinaryWrite(batch_meta_positions.size());
    for (size_t i = 0; i < batch_meta_positions.size(); ++i) {
        mf_writer.BinaryWrite(batch_meta_positions[i]);
    }

    mf_writer.BinaryWrite(pos);
}

MfFileReader::MfFileReader(std::ifstream& mf_stream, std::ofstream& csv_stream, std::ofstream& schema_stream)
    : mf_reader(mf_stream), csv_writer(csv_stream), schema_writer(schema_stream) {}

void MfFileReader::ProcessBatches(const Schema& schema) {
    size_t batch_count;
    mf_reader.BinaryRead(batch_count);
    batch_meta_positions.resize(batch_count);
    for (size_t i = 0; i < batch_meta_positions.size(); ++i) {
        mf_reader.BinaryRead(batch_meta_positions[i]);
    }

    for (size_t i = 0; i < batch_meta_positions.size(); ++i) {
        mf_reader.SetPos(batch_meta_positions[i]);
        size_t batch_column_start;
        mf_reader.BinaryRead(batch_column_start);
        mf_reader.SetPos(batch_column_start);

        Batch batch(schema, batch_rows_count);
        if (!batch_serialization::ReadMfBatch(mf_reader, batch)) {
            throw std::runtime_error("wrong batch format");
        }
        batch_serialization::WriteCsvBatch(batch, csv_writer);
    }
}

void MfFileReader::Convert() {
    Schema schema;
    size_t pos = mf_reader.ReadLastBytes();

    mf_reader.SetPos(pos);
    size_t column_count = 0;
    mf_reader.BinaryRead(column_count);
    schema.ReadSchema(mf_reader, column_count);
    schema.PrintSchema(schema_writer);

    ProcessBatches(schema);
}
