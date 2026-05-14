#pragma once

#include "utils.h"
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"

struct Schema {
    Schema();
    Schema(std::vector<std::string>&& names_, std::vector<Type>&& types_);
    void ReadSchema(Reader& type_reader, size_t column_count_ = Constants::MAX_COLUMN_COUNT); // any format
    static Type ValidateType(std::string_view type);
    size_t NumColumns() const;
    void PrintSchema(Writer& writer) const;
    void AddColumn(const std::string& name, Type type);
    const std::string& ColumnNameAt(size_t column_index) const;
    Type ColumnTypeAt(size_t column_index) const;
    std::optional<std::pair<Type, size_t>> GetTypeAndPos(const std::string& name) const;

private:
    std::vector<std::string> names;
    std::vector<Type> types;
    size_t column_count;
};
