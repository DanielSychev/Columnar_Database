#include <iostream>
#include <string>
#include <variant>
#include <vector>
#include <CSV_reader/reader.h>
#include <CSV_writer/writer.h>
#include <engine/data_storage.h>

int main() {
    std::string schema_path = "//Users//mac//Columnar_Database//src//TestFiles//schema.csv";
    std::ifstream schema_reader = std::ifstream(schema_path);
    Reader type_reader(schema_reader);
    DataStorage ds;
    std::vector<std::string> res;
    while(type_reader.ReadLine(res)) {
        if (res.size() != 2) {
            std::cerr << "pepe watafa" << std::endl;
            exit(0);
        }
        ds.AddColumn(res[0], res[1]);
    }
    std::string data_path = "//Users//mac//Columnar_Database//src//TestFiles//data.csv";
    std::ifstream data_reader = std::ifstream(data_path);
    Reader row_reader(data_reader);
    size_t col_num = ds.ColSize();
    // size_t i = 0; // номер строки (for row group)
    while (row_reader.ReadLine(res)) {
        if (res.size() != col_num) {
            std::cerr << "wrong pepe format" << std::endl;
            exit(0);
        }
        for (size_t j = 0; j < col_num; ++j) {
            ds.AddElem(j, res[j]);
        }
        // ++i;
    }
    Writer sneaky_writer(std::cout);
    ds.Print(sneaky_writer);

    // std::stringstream ss;
    // ss << "aaa" << "pepe\n";
    // Writer wr(ss);
}