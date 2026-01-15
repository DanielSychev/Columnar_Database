#include "engine/data_storage.h"
#include "CSV_writer/writer.h"

template<class... Ts>
struct overloaded : Ts... { using Ts::operator()...; };

template<class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

void DataStorage::AddColumn(std::string name, std::string type) {
    if (type == "int64") {
        table.push_back(std::vector<int64_t>());
    } else {
        table.push_back(std::vector<std::string>());
    }
    col_names.push_back(name);
}

size_t DataStorage::ColSize() const {
    return col_names.size();
}

void DataStorage::AddElem(size_t j, std::string elem) {
    if (auto v = std::get_if<std::vector<int64_t>>(&table[j])) {
        v->push_back(std::stoll(elem));
    } else if (auto v = std::get_if<std::vector<std::string>>(&table[j])) {
        v->push_back(elem);
    }
}

void DataStorage::PrintRow(Writer& w, size_t j) const {
    std::visit([&w](auto& v) {
        for (size_t i = 0; i < v.size(); ++i) {
            w.WriteElem(v[i], i == v.size() - 1);
        }
    }, table[j]);
}

void DataStorage::Print(Writer& w) const {
    for (size_t j = 0; j < table.size(); ++j) {
        PrintRow(w, j);
    }
}
