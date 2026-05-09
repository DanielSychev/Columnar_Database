#include "engine/engine.h"
#include "engine/serialization/batch_serialization.h"
#include <iostream>

Engine::Engine(std::ifstream& data_reader_stream, std::ofstream& data_writer_stream, std::ifstream& schema_reader_stream) :
data_reader(data_reader_stream), data_writer(data_writer_stream), type_reader(schema_reader_stream), type_writer(EMPTY_OUTPUT_STREAM) {}

Engine::Engine(std::ifstream& data_reader_stream, std::ofstream& data_writer_stream, std::ofstream& schema_writer_stream) :
data_reader(data_reader_stream), data_writer(data_writer_stream), type_reader(EMPTY_INPUT_STREAM), type_writer(schema_writer_stream) {}


void Engine::CsvToMfBatchProcessor(const Schema& schema) {
    while (true) {
        Batch batch(schema, batch_rows_count);
        if (!batch_serialization::ReadCsvBatch(data_reader, batch)) {
            break;
        }
        size_t meta_pos = batch_serialization::WriteMfBatch(batch, data_writer);
        batch_meta_positions.push_back(meta_pos);
    }
}

void Engine::CsvToMfProcessor() {
    Schema schema;
    schema.ReadSchema(type_reader);
    if (schema.NumColums() == 0) {
        throw std::runtime_error("schema is empty or was not read");
    }
    CsvToMfBatchProcessor(schema);

    size_t pos = data_writer.TellPos(); // начинаем писать мету + пишем схему
    data_writer.BinaryWrite(schema.NumColums());
    schema.PrintSchema(data_writer);
    
    data_writer.BinaryWrite(batch_meta_positions.size()); // пишем количество батчей и позиции начал их меты
    for (size_t i = 0; i < batch_meta_positions.size(); ++i) {
        data_writer.BinaryWrite(batch_meta_positions[i]);
        // std::cout << batch_positions[i] << std::endl;
    }
    
    data_writer.BinaryWrite(pos); // пишем метку в конце, откуда надо читать мету
}

void Engine::MfToCsvBatchProcessor(const Schema& schema) {
    size_t batch_count; // читаем позиции батчей
    data_reader.BinaryRead(batch_count);
    batch_meta_positions.resize(batch_count);
    for (size_t i = 0; i < batch_meta_positions.size(); ++i) {
        data_reader.BinaryRead(batch_meta_positions[i]);
        // std::cout << batch_positions[i] << std::endl;
    }

    for (size_t i = 0; i < batch_meta_positions.size(); ++i) { // читаем из my_format и пишем батчи в csv
        data_reader.SetPos(batch_meta_positions[i]);
        size_t batch_column_start;
        data_reader.BinaryRead(batch_column_start);
        data_reader.SetPos(batch_column_start);

        Batch batch(schema, batch_rows_count);
        
        if (!batch_serialization::ReadMfBatch(data_reader, batch)) {
            throw std::runtime_error("wrong batch format");
        }
        batch_serialization::WriteCsvBatch(batch, data_writer);
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
