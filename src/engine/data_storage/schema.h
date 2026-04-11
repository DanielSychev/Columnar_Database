#pragma once

#include <optional>
#include <string_view>
#include <vector>
// #include <utils.h>
#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"

enum class Type {
    int64, str
};

std::string_view TypeToString(Type t);

struct Schema {
    Schema();
    Schema(std::vector<std::string>&& names_, std::vector<Type>&& types_);
    void ReadSchema(Reader& type_reader, size_t column_count_ = Constants::MAX_COLUMN_COUNT); // any format
    static Type ValidateType(std::string_view type);
    size_t NumColums() const;
    void PrintSchema(Writer& writer) const;
    void AddColumn(const std::string& name, Type type);
    std::optional<std::pair<Type, size_t>> GetTypeAndPos(const std::string& name);

    std::vector<std::string> names;
    std::vector<Type> types;
    size_t column_count; // columns_count
};
