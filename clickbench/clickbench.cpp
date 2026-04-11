#include "engine/serialization/batch_serialization.h"
#include "queries_executor/operator.h"
#include "queries_executor/executor.h"
#include <fstream>
#include <memory>
#include <stdexcept>

namespace {
std::unique_ptr<std::ifstream> data_stream;
std::shared_ptr<ScanOperator> scanner;
}

void MakeScanOperator() {
    std::string data_path = "//Users//mac//Columnar_Database//src//TestFiles//output.mf";
    data_stream = std::make_unique<std::ifstream>(data_path, std::ios::binary);
    if (!data_stream->is_open()) {
        throw std::runtime_error("cannot open data file: " + data_path);
    }
}

// SELECT COUNT(*) FROM hits;
std::shared_ptr<Operator> MakeQuery1() {
    std::vector<std::string> columns = {"AdvEngineID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    return std::make_shared<CountOperator>(scanner);
}

// SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;
std::shared_ptr<Operator> MakeQuery2() {
    std::vector<std::string> columns = {"AdvEngineID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    return std::make_shared<CountOperator>(scanner);
}

int main() {
    MakeScanOperator();
    std::shared_ptr<Operator> queries[43];
    queries[0] = MakeQuery1();
    // queries[1] = MakeQuery2();
    for (int i = 0; i < 1; ++i) {
        auto executor = ExecuteOperator(queries[i]);
        std::ofstream result_stream("//Users//mac//Columnar_Database//src//TestFiles//result"+std::to_string(i)+".csv");
        Writer result_writer(result_stream);
        while (auto batch = executor->NextBatch()) {
            batch_serialization::WriteCsvBatch(*batch, result_writer);
        }
    }
    return 0;
}
