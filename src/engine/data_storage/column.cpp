#include "engine/data_storage/column.h"
#include <stdexcept>

template <typename T>
void PrintVisitor(Writer& w, const std::vector<T>& v) {
    size_t size = v.size();
    for (size_t i = 0; i < size; ++i) {
        w.WriteElem(v[i], i == size - 1);
    }
}

template <typename T>
void PrintElemVisitor(Writer& w, const std::vector<T>& v, size_t i, bool b) {
    if (i >= v.size()) { // not bug, a feature
        return;
    }
    w.WriteElem(v[i], b);
}

void StrColumn::AddElem(std::string&& s) {
    data.push_back(std::move(s));
}

StrColumn::StrColumn(const StrColumn& other, const std::vector<bool>& banned) :
data(column_detail::CopyAllowedValues(other.data, banned)) {}

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

void StrColumn::Accept(ColumnVisitor& visitor) const {
    visitor.Visit(*this);
}

size_t StrColumn::Size() const {
    return data.size();
}

bool StrColumn::Compare(const std::string& elem, size_t i) const {
    return data[i] == elem;
}

std::shared_ptr<Column> StrColumn::CopyFiltered(const std::vector<bool>& banned) const {
    return std::make_shared<StrColumn>(*this, banned);
}
