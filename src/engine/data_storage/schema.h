#pragma once

#include <cstdint>
#include <vector>
#include "CSV_reader/reader.h"
#include "CSV_writer/writer.h"

const size_t max_col_count = UINT32_MAX;

enum class Type {
    int64, str
};

std::string TypeToString(const Type& t);

struct Schema {
    Schema();
    void ReadSchema(Reader& type_reader, size_t col_count_ = max_col_count); // any format
    Type ValidateType(const std::string& type);
    size_t NumColums();
    void PrintSchema(Writer& writer);

    std::vector<std::string> names;
    std::vector<Type> types;
    size_t col_count; // columns_count
};