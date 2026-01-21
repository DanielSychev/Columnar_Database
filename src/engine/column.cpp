#include "engine/column.h"

template <typename T>
void PrintVisitor(Writer& w, const std::vector<T>& v) {
    for (size_t i = 0; i < v.size(); ++i) {
        w.WriteElem(v[i], i == v.size() - 1);
    }
}

void Int64Column::AddElem(std::string&& s) {
    data.push_back(std::stoll(s)); // stoll сам делает throw в случае чего
}

void Int64Column::Print(Writer& w) {
    PrintVisitor(w, data);
}

void StrColumn::AddElem(std::string&& s) {
    data.push_back(std::move(s));
}

void StrColumn::Print(Writer& w) {
    PrintVisitor(w, data);
}