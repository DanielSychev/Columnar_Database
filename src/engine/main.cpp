#include <iostream>
#include <string>
#include <CSV_reader/reader.h>
#include <CSV_writer/writer.h>
#include <engine/data_storage.h>
#include <engine/schema.h>
#include <engine/batch.h>

const size_t batch_rows_count = 1;

int main() {
    std::string schema_path = "//Users//mac//Columnar_Database//src//TestFiles//schema.csv";
    std::ifstream schema_stream(schema_path);
    Reader type_reader(schema_stream);
    Schema schema;
    schema.ReadSchema(type_reader);

    std::ofstream writer_stream("//Users//mac//Columnar_Database//src//TestFiles//output.csv");
    Writer writer(writer_stream);
    schema.Print(writer);

    std::string data_path = "//Users//mac//Columnar_Database//src//TestFiles//data.csv";
    std::ifstream data_stream(data_path);
    Reader row_reader(data_stream);

    while (true) {
        Batch batch(schema);
        if (!batch.ReadBatch(row_reader, batch_rows_count)) {
            break;
        }
        batch.Print(writer);
    }

    // std::stringstream ss;
    // ss << "aaa" << "pepe\n";
    // Writer wr(ss);
}