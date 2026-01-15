#pragma once

#include <iostream>
#include <vector>
#include <variant>

class Writer;

class DataStorage {
public:
    void AddColumn(std::string name, std::string type);

    size_t ColSize() const;

    void AddElem(size_t j, std::string elem);

    void PrintRow(Writer& w, size_t j) const;

    void Print(Writer& w) const;
private:
    std::vector<std::variant<std::vector<int64_t>, std::vector<std::string>>> table;
    std::vector<std::string> col_names;
};