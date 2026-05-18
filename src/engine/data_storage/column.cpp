#include "engine/data_storage/column.h"
#include <stdexcept>
#include <string>

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

namespace {
bool LikeCompare(std::string_view value, std::string_view pattern) {
    if (pattern.find('_') == std::string_view::npos) {
        const size_t first_pct = pattern.find('%');

        if (first_pct == std::string_view::npos) {
            return value == pattern;
        }

        const size_t last_pct  = pattern.rfind('%');
        const std::string_view prefix = pattern.substr(0, first_pct);
        const std::string_view suffix = pattern.substr(last_pct + 1);
        const std::string_view inner  = pattern.substr(first_pct + 1, last_pct - first_pct - 1);

        if (inner.find('%') == std::string_view::npos) {
            if (!value.starts_with(prefix)) return false;
            if (!value.ends_with(suffix))   return false;
            if (value.size() < prefix.size() + suffix.size()) return false;

            if (first_pct == last_pct) {
                return true;
            }

            if (inner.empty()) return true;
            const std::string_view mid = value.substr(
                prefix.size(),
                value.size() - prefix.size() - suffix.size()
            );
            return mid.find(inner) != std::string_view::npos;
        }
    }

    std::vector<bool> previous(pattern.size() + 1, false);
    std::vector<bool> current(pattern.size() + 1, false);
    previous[0] = true;

    for (size_t pattern_pos = 1; pattern_pos <= pattern.size(); ++pattern_pos) {
        if (pattern[pattern_pos - 1] == '%') {
            previous[pattern_pos] = previous[pattern_pos - 1];
        }
    }

    for (size_t value_pos = 1; value_pos <= value.size(); ++value_pos) {
        current[0] = false;
        for (size_t pattern_pos = 1; pattern_pos <= pattern.size(); ++pattern_pos) {
            if (pattern[pattern_pos - 1] == '%') {
                current[pattern_pos] = current[pattern_pos - 1] || previous[pattern_pos];
            } else if (pattern[pattern_pos - 1] == '_' || pattern[pattern_pos - 1] == value[value_pos - 1]) {
                current[pattern_pos] = previous[pattern_pos - 1];
            } else {
                current[pattern_pos] = false;
            }
        }
        std::swap(previous, current);
        std::fill(current.begin(), current.end(), false);
    }

    return previous[pattern.size()];
}
}

std::shared_ptr<Column> CreateColumn(Type type) {
    switch (type) {
    case Type::int128:
        return std::make_shared<Int128Column>();
    case Type::int64:
        return std::make_shared<Int64Column>();
    case Type::int32:
        return std::make_shared<Int32Column>();
    case Type::int16:
        return std::make_shared<Int16Column>();
    case Type::int8:
        return std::make_shared<Int8Column>();
    case Type::double_:
        return std::make_shared<DoubleColumn>();
    case Type::str:
        return std::make_shared<StrColumn>();
    case Type::date:
        return std::make_shared<DateColumn>();
    case Type::timestamp:
        return std::make_shared<TimeStampColumn>();
    default:
        throw std::runtime_error("unknown type was given (in CreateColumn)");
    }
}

std::shared_ptr<Column> CreateColumn(Type type, const std::vector<std::string>& values) {
    switch (type) {
    case Type::int128:
        return std::make_shared<Int128Column>(values);
    case Type::int64:
        return std::make_shared<Int64Column>(values);
    case Type::int32:
        return std::make_shared<Int32Column>(values);
    case Type::int16:
        return std::make_shared<Int16Column>(values);
    case Type::int8:
        return std::make_shared<Int8Column>(values);
    case Type::double_:
        return std::make_shared<DoubleColumn>(values);
    case Type::str:
        return std::make_shared<StrColumn>(values);
    case Type::date:
        return std::make_shared<DateColumn>(values);
    case Type::timestamp:
        return std::make_shared<TimeStampColumn>(values);
    default:
        throw std::runtime_error("unknown type was given (in CreateColumn)");
    }
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

std::string StrColumn::GetElemToString(size_t index) const {
    if (index >= data.size()) {
        throw std::out_of_range("index out of range in GetElemToString");
    }
    return data[index];
}

void StrColumn::Accept(ColumnVisitor& visitor, size_t ind) const {
    visitor.Visit(*this, ind);
}

size_t StrColumn::Size() const {
    return data.size();
}

bool StrColumn::Compare(const std::string& elem, size_t i, CompareSign sign) const {
    if (sign == CompareSign::IN) {
        throw std::runtime_error("IN sign is not supported for StrColumn");
    }
    if (sign == CompareSign::LIKE) {
        return LikeCompare(data[i], elem);
    }
    if (sign == CompareSign::NOT_LIKE) {
        return !LikeCompare(data[i], elem);
    }
    return column_detail::BasicCompare(data[i], elem, sign);
}

std::shared_ptr<Column> StrColumn::CopyFiltered(const std::vector<bool>& banned) const {
    return std::make_shared<StrColumn>(*this, banned);
}

std::shared_ptr<Column> StrColumn::CopyReordered(const std::vector<size_t>& ordered) const {
    return std::make_shared<StrColumn>(column_detail::CopyReorderedValues(data, ordered));
}

const std::vector<std::string>& StrColumn::Data() const {
    return data;
}

std::string StrColumn::ValueAt(size_t index) const {
    if (index >= data.size()) {
        throw std::out_of_range("index out of range in ValueAt");
    }
    return data[index];
}

std::shared_ptr<Column> TimeStampColumn::CopyReordered(const std::vector<size_t>& ordered) const {
    return std::make_shared<TimeStampColumn>(column_detail::CopyReorderedValues(data, ordered));
}

void TimeStampColumn::Accept(ColumnVisitor& visitor, size_t ind) const {
    visitor.Visit(*this, ind);
}

std::shared_ptr<Column> DateColumn::CopyReordered(const std::vector<size_t>& ordered) const {
    return std::make_shared<DateColumn>(column_detail::CopyReorderedValues(data, ordered));
}

void DateColumn::Accept(ColumnVisitor& visitor, size_t ind) const {
    visitor.Visit(*this, ind);
}
