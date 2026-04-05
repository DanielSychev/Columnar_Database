#include "engine/data_storage/column.h"
#include <stdexcept>

template <typename T>
void PrintVisitor(Writer& w, const std::vector<T>& v) {
    for (size_t i = 0; i < v.size(); ++i) {
        w.WriteElem(v[i], i == v.size() - 1);
    }
}

template <typename T>
void PrintElemVisitor(Writer& w, const std::vector<T>& v, size_t i, bool b) {
    if (i >= v.size()) { // not bug, a feature
        return;
    }
    w.WriteElem(v[i], b);
}

void Int64Column::AddElem(std::string&& s) {
    data.push_back(std::stoll(s)); // stoll сам делает throw в случае чего
}

void Int64Column::Print(Writer& w) const {
    w.BinaryWriteVector(data);
}

void Int64Column::Read(Reader& r) {
    r.BinaryReadVector(data);
}

void Int64Column::PrintElem(Writer& w, size_t i, bool b) const {
    PrintElemVisitor(w, data, i, b);
}

size_t Int64Column::Size() const {
    return data.size();
}

void StrColumn::AddElem(std::string&& s) {
    data.push_back(std::move(s));
}

void StrColumn::Print(Writer& w) const {
    PrintVisitor(w, data);
}

void StrColumn::Read(Reader& r) {
    if (!r.ReadLine(data)) {
        throw std::runtime_error("wrong batch format");
    }
}

void StrColumn::PrintElem(Writer& w, size_t i, bool b) const {
    PrintElemVisitor(w, data, i, b);
}

size_t StrColumn::Size() const {
    return data.size();
}
