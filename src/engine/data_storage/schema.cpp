#include "engine/data_storage/schema.h"
#include <optional>
#include <stdexcept>

Schema::Schema() : column_count(0) {}

Schema::Schema(std::vector<std::string>&& names_, std::vector<Type>&& types_) : names(std::move(names_)), types(std::move(types_)), column_count(names.size()) {}


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

void Schema::ReadSchema(Reader& type_reader, size_t column_count_) {
    names.clear();
    types.clear();
    std::vector<std::string> res;
    size_t i = 0;
    while (i < column_count_ && type_reader.ReadLine(res)) {
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
    column_count = i;
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
    return column_count;
}

void Schema::PrintSchema(Writer& writer) const {
    for (size_t i = 0; i < column_count; ++i) {
        writer.WriteElem(names[i], false);
        writer.WriteElem(TypeToString(types[i]), true);
    }
}

void Schema::AddColumn(const std::string& name, Type type) {
    names.push_back(name);
    types.push_back(type);
    ++column_count;
}

std::optional<std::pair<Type, size_t>> Schema::GetTypeAndPos(const std::string& name) {
    for (size_t i = 0; i < column_count; ++i) {
        if (names[i] == name) {
            return std::make_pair(types[i], i);
        }
    }
    return std::nullopt;
}