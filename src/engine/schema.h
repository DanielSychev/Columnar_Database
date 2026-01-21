#pragma once

#include <vector>
#include "CSV_reader/reader.h"
#include "CSV_writer/writer.h"

enum class Type {
    int64, str
};

std::string TypeToString(const Type& t);

struct Schema {
    Schema();
    void ReadSchema(Reader& type_reader);
    Type ValidateType(const std::string& type);
    size_t NumColums();
    void Print(Writer& writer);

    std::vector<std::string> names;
    std::vector<Type> types;
    size_t col_count; // columns_count
};