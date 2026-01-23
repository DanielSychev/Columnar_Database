#include <iostream>
#include <string>
#include <CSV_reader/reader.h>
#include <CSV_writer/writer.h>
#include <engine/data_storage/schema.h>
#include <engine/data_storage/batch.h>
#include <engine/engine.h>

std::string schema_path = "//Users//mac//Columnar_Database//src//TestFiles//schema_input.csv";
std::string schema_writer_path = "//Users//mac//Columnar_Database//src//TestFiles//schema_output.csv";
std::string data_writer_path = "//Users//mac//Columnar_Database//src//TestFiles//output.mf";
std::string data_path = "//Users//mac//Columnar_Database//src//TestFiles//data_input.csv";

int main(int argc, char** argv) {
    if (argc < 1) {
        std::cout << "Wrong parametrs";
        return 0;
    }
    std::cout << argv[1] << std::endl;
    if (strcmp(argv[1], "0") == 0) {
        std::ifstream schema_stream(schema_path);
        std::ofstream writer_stream(data_writer_path);
        std::ifstream data_stream(data_path);
        Engine engine(data_stream, writer_stream, schema_stream);
        engine.CsvToMfProcessor();
    } else if (strcmp(argv[1], "1") == 0) {
        std::string data_writer_path = "//Users//mac//Columnar_Database//src//TestFiles//data_output.csv";
        std::ofstream data_writer_stream(data_writer_path);
        std::string data_path = "//Users//mac//Columnar_Database//src//TestFiles//output.mf";
        std::ifstream data_stream(data_path);
        std::ofstream scheme_writer_stream(schema_writer_path);
        Engine engine(data_stream, data_writer_stream, scheme_writer_stream);
        engine.MfToCsvProcessor();
    } else {
        std::cout << "wtf" << std::endl;
    }
}