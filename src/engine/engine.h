#include <fstream>
#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"
#include <engine/data_storage/schema.h>
#include <engine/data_storage/batch.h>

static std::ifstream EMPTY_INPUT_STREAM;
static std::ofstream EMPTY_OUTPUT_STREAM;

class Engine {
public:
    Engine(std::ifstream& data_reader_stream, std::ofstream& data_writer_stream, std::ifstream& schema_reader_stream);
    Engine(std::ifstream& data_reader_stream, std::ofstream& data_writer_stream, std::ofstream& schema_writer_stream);
    void CsvToMfProcessor();
    void MfToCsvProcessor();
private:
    void CsvToMfBatchProcessor(Schema& schema);
    void MfToCsvBatchProcessor(Schema& schema);
    Reader data_reader;
    Writer data_writer;
    Reader type_reader;
    Writer type_writer;
    const size_t batch_rows_count = 1000;
    std::vector<size_t> batch_positions;
};