#include "schema.h"
#include <stdexcept>

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

void Schema::ReadSchema(Reader& type_reader) {
    std::vector<std::string> res;
    while(type_reader.ReadLine(res)) {
        if (res.size() != 2) {
            throw std::runtime_error("bad schema was given");
            exit(0);
        }
        names.push_back(std::move(res[0]));
        types.push_back(ValidateType(res[1]));
    }
    col_count = names.size();
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

void Schema::Print(Writer& writer) {
    for (size_t i = 0; i < col_count; ++i) {
        writer.WriteElem(names[i], false);
        writer.WriteElem(TypeToString(types[i]), true);
    }
}