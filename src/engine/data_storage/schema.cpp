#include "engine/data_storage/schema.h"
#include <stdexcept>

Schema::Schema() : column_count(0) {}

Schema::Schema(std::vector<std::string>&& names_, std::vector<Type>&& types_)
    : names(std::move(names_)), types(std::move(types_)), column_count(names.size()) {
    if (names.size() != types.size()) {
        throw std::runtime_error("schema names and types sizes differ");
    }
}


std::string_view TypeToString(Type t) {
    switch (t) {
        case Type::int128:
            return "int128";
        case Type::int64:
            return "int64";
        case Type::str:
            return "string";
        case Type::int32:
            return "int32";
        case Type::int16:
            return "int16";
        case Type::int8:
            return "int8";
        case Type::double_:
            return "double";
        case Type::date:
            return "DATE";
        case Type::timestamp:
            return "TIMESTAMP";
        default:
            return "another type";
    }
}

void Schema::ReadSchema(Reader& type_reader, size_t column_count_) {
    names.clear();
    types.clear();
    column_count = 0;
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
    if (type == "int128") {
        return Type::int128;
    }
    if (type == "int64") {
        return Type::int64;
    }
    if (type == "int32") {
        return Type::int32;
    }
    if (type == "int16") {
        return Type::int16;
    }
    if (type == "int8") {
        return Type::int8;
    }
    if (type == "double") {
        return Type::double_;
    }
    if (type == "string") {
        return Type::str;
    }
    if (type == "DATE") {
        return Type::date;
    }
    if (type == "TIMESTAMP") {
        return Type::timestamp;
    }
    throw std::runtime_error("not existing type was given");
}

size_t Schema::NumColumns() const {
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

const std::string& Schema::ColumnNameAt(size_t column_index) const {
    if (column_index >= column_count) {
        throw std::out_of_range("wrong schema column index");
    }
    return names[column_index];
}

Type Schema::ColumnTypeAt(size_t column_index) const {
    if (column_index >= column_count) {
        throw std::out_of_range("wrong schema column index");
    }
    return types[column_index];
}

std::optional<std::pair<Type, size_t>> Schema::GetTypeAndPos(const std::string& name) const {
    for (size_t i = 0; i < column_count; ++i) {
        if (names[i] == name) {
            return std::make_pair(types[i], i);
        }
    }
    return std::nullopt;
}

bool Schema::HasColumn(const std::string& name) const {
    for (size_t i = 0; i < column_count; ++i) {
        if (names[i] == name) {
            return true;
        }
    }
    return false;
}