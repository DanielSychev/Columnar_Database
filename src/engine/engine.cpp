#include "engine/engine.h"
#include <stdexcept>
#include <string>

Engine::Engine(std::ifstream& data_stream, std::ofstream& data_writer_stream, std::ifstream& schema_stream) :
data_reader(data_stream), data_writer(data_writer_stream), type_reader(schema_stream), type_writer(EMPTY_OUTPUT_STREAM) {}

Engine::Engine(std::ifstream& data_stream, std::ofstream& data_writer_stream, std::ofstream& schema_writer_stream) :
data_reader(data_stream), data_writer(data_writer_stream), type_reader(EMPTY_INPUT_STREAM), type_writer(schema_writer_stream) {}


void Engine::CsvToMfBatchProcessor(Schema& schema) {
    while (true) {
        Batch batch(schema, batch_rows_count);
        if (!batch.CSVReadBatch(data_reader)) {
            break;
        }
        batch_positions.push_back(data_writer.TellPos());
        batch.MFPrintBatch(data_writer);
    }
}

void Engine::CsvToMfProcessor() {
    Schema schema; // пишем батчи
    schema.ReadSchema(type_reader);
    CsvToMfBatchProcessor(schema);

    size_t pos = data_writer.TellPos(); // начинаем писать мету + пишем схему
    data_writer.BinaryWrite(schema.NumColums());
    schema.PrintSchema(data_writer);
    
    data_writer.BinaryWrite(batch_positions.size()); // пишем количество батчей и позиции
    for (size_t i = 0; i < batch_positions.size(); ++i) {
        data_writer.BinaryWrite(batch_positions[i]);
        std::cout << batch_positions[i] << std::endl;
    }
    
    data_writer.BinaryWrite(pos); // пишем метку в конце, откуда надо читать мету
}

void Engine::MfToCsvBatchProcessor(Schema& schema) {
    size_t batch_count; // читаем позиции батчей
    data_reader.BinaryRead(batch_count);
    batch_positions.resize(batch_count);
    for (size_t i = 0; i < batch_positions.size(); ++i) {
        data_reader.BinaryRead(batch_positions[i]);
        std::cout << batch_positions[i] << std::endl;
    }

    for (size_t i = 0; i < batch_positions.size(); ++i) { // читаем из my_format и пишем батчи в csv
        data_reader.SetPos(batch_positions[i]);
        Batch batch(schema, batch_rows_count);
        batch.MFReadBatch(data_reader);
        batch.CSVPrintBatch(data_writer);
    }
}

void Engine::MfToCsvProcessor() {
    Schema schema;
    size_t pos = data_reader.ReadLastBytes(); // позиция меты
    
    data_reader.SetPos(pos); // читаем схему
    size_t column_count = 0;
    data_reader.BinaryRead(column_count);
    schema.ReadSchema(data_reader, column_count);
    schema.PrintSchema(type_writer);
    
    MfToCsvBatchProcessor(schema); // восстанавливаем батчи
}