#include "engine/data_storage/schema.h"
#include <stdexcept>
#include <iostream>

Schema::Schema() : col_count(0) {}

std::string TypeToString(const Type &t) {
    switch (t) {
        case Type::int64:
            return std::string("int64");
        case Type::str:
            return "string";
        default:
            return "another type";
    }
}

void Schema::ReadSchema(Reader& type_reader, size_t col_count_) {
    std::vector<std::string> res;
    size_t i = 0;
    while(i < col_count_ && type_reader.ReadLine(res)) {
        if (res.size() != 2) {
            throw std::runtime_error("bad schema was given");
            exit(0);
        }
        names.push_back(std::move(res[0]));
        types.push_back(ValidateType(res[1]));
        ++i;
    }
    col_count = i;
}

Type Schema::ValidateType(const std::string& type) {
    if (type == "int64") {
        return Type::int64;
    }
    if (type == "string") {
        return Type::str;
    }
    throw std::runtime_error("not existing type was given");
}

size_t Schema::NumColums() {
    return col_count;
}

void Schema::PrintSchema(Writer& writer) {
    for (size_t i = 0; i < col_count; ++i) {
        writer.WriteElem(names[i], false);
        writer.WriteElem(TypeToString(types[i]), true);
    }
}