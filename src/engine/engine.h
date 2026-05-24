#pragma once

#include <cstddef>
#include <fstream>
#include <vector>
#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"
#include "engine/data_storage/schema.h"
#include "utils.h"

class MfFileWriter {
public:
    MfFileWriter(std::ifstream& csv_stream, std::ofstream& mf_stream, std::ifstream& schema_stream);
    void Convert();
private:
    void ProcessBatches(const Schema& schema);
    Reader csv_reader;
    Writer mf_writer;
    Reader schema_reader;
    std::vector<size_t> batch_meta_positions;
    const size_t batch_rows_count = Constants::BATCH_SIZE;
};

class MfFileReader {
public:
    MfFileReader(std::ifstream& mf_stream, std::ofstream& csv_stream, std::ofstream& schema_stream);
    void Convert();
private:
    void ProcessBatches(const Schema& schema);
    Reader mf_reader;
    Writer csv_writer;
    Writer schema_writer;
    std::vector<size_t> batch_meta_positions;
    const size_t batch_rows_count = Constants::BATCH_SIZE;
};
