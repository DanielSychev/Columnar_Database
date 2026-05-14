#include <chrono>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <engine/engine.h>

namespace {
constexpr std::string_view default_schema_input_path = "src/TestFiles/schema_sample.csv";
constexpr std::string_view default_csv_input_path = "src/TestFiles/hits_sample.csv";
constexpr std::string_view default_mf_output_path = "src/TestFiles/output.mf";
constexpr std::string_view default_mf_input_path = "src/TestFiles/output.mf";
constexpr std::string_view default_csv_output_path = "src/TestFiles/data_output.csv";
constexpr std::string_view default_schema_output_path = "src/TestFiles/schema_output.csv";

void Helper(const char* executable) {
    std::cout
        << "Usage:\n"
        << "  " << executable << " 0 [schema.csv] [input.csv] [output.mf]\n"
        << "  " << executable << " 1 [input.mf] [output.csv] [schema_output.csv]\n";
}

std::string GetArgument(int argc, char** argv, int index, std::string_view default_value) {
    if (argc > index) {
        return argv[index];
    }
    return std::string(default_value);
}

void EnsureNoExtraArguments(int argc, int max_argc) {
    if (argc > max_argc) {
        throw std::runtime_error("too many arguments");
    }
}

void RunCsvToMf(const std::string& schema_path, const std::string& data_path, const std::string& mf_output_path) {
    std::ifstream schema_stream(schema_path);
    std::ifstream data_stream(data_path);
    std::ofstream writer_stream(mf_output_path, std::ios::binary);

    if (!schema_stream.is_open()) {
        throw std::runtime_error("cannot open schema file: " + schema_path);
    }
    if (!data_stream.is_open()) {
        throw std::runtime_error("cannot open data file: " + data_path);
    }
    if (!writer_stream.is_open()) {
        throw std::runtime_error("cannot open output file: " + mf_output_path);
    }

    Engine engine(data_stream, writer_stream, schema_stream);
    const auto start_time = std::chrono::steady_clock::now();
    engine.CsvToMfProcessor();
    const auto end_time = std::chrono::steady_clock::now();
    const std::chrono::duration<double> elapsed = end_time - start_time;
    std::cout << "CSV to MF took " << elapsed.count() << " seconds\n";
}

void RunMfToCsv(const std::string& mf_input_path, const std::string& csv_output_path, const std::string& schema_output_path) {
    std::ifstream data_stream(mf_input_path, std::ios::binary);
    std::ofstream data_writer_stream(csv_output_path);
    std::ofstream scheme_writer_stream(schema_output_path);

    if (!data_stream.is_open()) {
        throw std::runtime_error("cannot open data file: " + mf_input_path);
    }
    if (!data_writer_stream.is_open()) {
        throw std::runtime_error("cannot open output file: " + csv_output_path);
    }
    if (!scheme_writer_stream.is_open()) {
        throw std::runtime_error("cannot open output file: " + schema_output_path);
    }

    Engine engine(data_stream, data_writer_stream, scheme_writer_stream);
    const auto start_time = std::chrono::steady_clock::now();
    engine.MfToCsvProcessor();
    const auto end_time = std::chrono::steady_clock::now();
    const std::chrono::duration<double> elapsed = end_time - start_time;
    std::cout << "MF to CSV took " << elapsed.count() << " seconds\n";
}
}

int main(int argc, char** argv) {
    try {
        if (argc < 2) {
            Helper(argv[0]);
            return 1;
        }

        const std::string_view mode = argv[1];
        if (mode == "0") {
            EnsureNoExtraArguments(argc, 5);
            RunCsvToMf(
                GetArgument(argc, argv, 2, default_schema_input_path),
                GetArgument(argc, argv, 3, default_csv_input_path),
                GetArgument(argc, argv, 4, default_mf_output_path)
            );
        } else if (mode == "1") {
            EnsureNoExtraArguments(argc, 5);
            RunMfToCsv(
                GetArgument(argc, argv, 2, default_mf_input_path),
                GetArgument(argc, argv, 3, default_csv_output_path),
                GetArgument(argc, argv, 4, default_schema_output_path)
            );
        } else {
            Helper(argv[0]);
            return 1;
        }
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
