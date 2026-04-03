#include <iostream>
#include <string>
#include <stdexcept>
#include <fstream>
#include <engine/data_storage/schema.h>
#include <engine/data_storage/batch.h>
#include <engine/engine.h>

std::string schema_path = "//Users//mac//Columnar_Database//src//TestFiles//scheme.csv";
std::string schema_writer_path = "//Users//mac//Columnar_Database//src//TestFiles//schema_output.csv";
std::string data_writer_path = "//Users//mac//Columnar_Database//src//TestFiles//output.mf";
std::string data_path = "//Users//mac//Columnar_Database//src//TestFiles//hits_sample.csv";

int main(int argc, char** argv) {
    try {
        if (argc < 2) {
            std::cerr << "Wrong parametrs\n";
            return 1;
        }

        if (strcmp(argv[1], "0") == 0) { // CSV -> MF (my format)
            std::ifstream schema_stream(schema_path);
            std::ofstream writer_stream(data_writer_path);
            std::ifstream data_stream(data_path);

            if (!schema_stream.is_open()) {
                throw std::runtime_error("cannot open schema file: " + schema_path);
            }
            if (!data_stream.is_open()) {
                throw std::runtime_error("cannot open data file: " + data_path);
            }
            if (!writer_stream.is_open()) {
                throw std::runtime_error("cannot open output file: " + data_writer_path);
            }
            
            Engine engine(data_stream, writer_stream, schema_stream);
            engine.CsvToMfProcessor();
        } else if (strcmp(argv[1], "1") == 0) { // MF -> CSV
            std::string data_writer_path = "//Users//mac//Columnar_Database//src//TestFiles//data_output.csv";
            std::ofstream data_writer_stream(data_writer_path);
            std::string data_path = "//Users//mac//Columnar_Database//src//TestFiles//output.mf";
            std::ifstream data_stream(data_path);
            std::ofstream scheme_writer_stream(schema_writer_path);

            if (!data_stream.is_open()) {
                throw std::runtime_error("cannot open data file: " + data_path);
            }
            if (!data_writer_stream.is_open()) {
                throw std::runtime_error("cannot open output file: " + data_writer_path);
            }
            if (!scheme_writer_stream.is_open()) {
                throw std::runtime_error("cannot open output file: " + schema_writer_path);
            }

            Engine engine(data_stream, data_writer_stream, scheme_writer_stream);
            engine.MfToCsvProcessor();
        } else {
            std::cerr << "give 0 to process into mf, or 1 to process into csv\n";
            return 1;
        }
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
