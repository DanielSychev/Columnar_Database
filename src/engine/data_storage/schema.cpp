#include "engine/data_storage/schema.h"
#include <stdexcept>

Schema::Schema() : col_count(0) {}

std::string_view TypeToString(Type t) {
    switch (t) {
        case Type::int64:
            return "int64";
        case Type::str:
            return "string";
        default:
            return "another type";
    }
}

void Schema::ReadSchema(Reader& type_reader, size_t col_count_) {
    names.clear();
    types.clear();
    std::vector<std::string> res;
    size_t i = 0;
    while (i < col_count_ && type_reader.ReadLine(res)) {
        if (res.size() == 3 && res[2].empty()) {
            res.pop_back();
        }
        if (res.size() != 2) {
            throw std::runtime_error("bad schema was given");
        }
        names.push_back(std::move(res[0]));
        types.push_back(ValidateType(res[1]));
        ++i;
    }
    col_count = i;
}

Type Schema::ValidateType(std::string_view type) {
    if (type == "int16" || type == "int32" || type == "int64") {
        return Type::int64;
    }
    if (type == "string" || type == "DATE" || type == "TIMESTAMP") {
        return Type::str;
    }
    throw std::runtime_error("not existing type was given");
}

size_t Schema::NumColums() const {
    return col_count;
}

void Schema::PrintSchema(Writer& writer) const {
    for (size_t i = 0; i < col_count; ++i) {
        writer.WriteElem(names[i], false);
        writer.WriteElem(TypeToString(types[i]), true);
    }
}
