#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"
#include "engine/data_storage/visitors/visitor.h"
#include "utils.h"

class Column {
public:
    virtual void AddElem(std::string&&) = 0; // чтобы move делать
    virtual void Print(Writer&) const = 0;
    virtual void Read(Reader&) = 0;
    virtual void PrintElem(Writer&, size_t, bool) const = 0;
    virtual std::string GetElemToString(size_t index) const = 0;
    virtual void Accept(ColumnVisitor& visitor) const = 0;
    virtual bool Compare(const std::string&, size_t, CompareSign) const = 0;
    virtual std::shared_ptr<Column> CopyFiltered(const std::vector<bool>& banned) const = 0;
    virtual std::shared_ptr<Column> CopyReordered(const std::vector<size_t>& ordered) const = 0;
    virtual size_t Size() const = 0;
    virtual ~Column() = default;
};

std::shared_ptr<Column> CreateColumn(Type type);
std::shared_ptr<Column> CreateColumn(Type type, const std::vector<std::string>& values);

namespace column_detail {
template<typename T> 
bool BasicCompare(const T& a, const T& b, CompareSign sign) {
    switch (sign) {
        case CompareSign::EQUAL:
            return a == b;
        case CompareSign::NOT_EQUAL:
            return a != b;
        case CompareSign::LESS:
            return a < b;
        case CompareSign::GREATER:
            return a > b;
        case CompareSign::LESS_OR_EQUAL:
            return a <= b;
        case CompareSign::GREATER_OR_EQUAL:
            return a >= b;
        default:
            throw std::invalid_argument("invalid CompareSign");
    }
}

template <typename T>
std::vector<T> CopyAllowedValues(const std::vector<T>& data, const std::vector<bool>& banned) {
    if (data.size() != banned.size()) {
        throw std::runtime_error("wrong filtered column format");
    }

    std::vector<T> filtered_data;
    filtered_data.reserve(data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        if (!banned[i]) {
            filtered_data.push_back(data[i]);
        }
    }
    return filtered_data;
}

template <typename T>
std::vector<T> CopyReorderedValues(const std::vector<T>& data, const std::vector<size_t>& ordered) {
    if (data.size() < ordered.size()) {
        throw std::runtime_error("wrong reordered column format");
    }

    std::vector<T> reordered_data;
    reordered_data.reserve(ordered.size());
    for (auto& index: ordered) {
        if (index >= data.size()) {
            throw std::out_of_range("index out of range in CopyReorderedValues");
        }
        reordered_data.push_back(data[index]);
    }
    return reordered_data;
}


inline __int128_t ParseInt128(std::string_view value) {
    if (value.empty()) {
        throw std::invalid_argument("empty __int128_t");
    }

    size_t pos = 0;
    bool negative = false;
    if (value[pos] == '+' || value[pos] == '-') {
        negative = value[pos] == '-';
        ++pos;
    }

    if (pos == value.size()) {
        throw std::invalid_argument("invalid __int128_t");
    }

    constexpr __uint128_t kInt128Max = static_cast<__uint128_t>(std::numeric_limits<__int128_t>::max());
    constexpr __uint128_t kInt128MinAbs = kInt128Max + 1;

    const __uint128_t limit = negative ? kInt128MinAbs : kInt128Max;
    __uint128_t result = 0;
    for (; pos < value.size(); ++pos) {
        const char ch = value[pos];
        if (ch < '0' || ch > '9') {
            throw std::invalid_argument("invalid __int128_t");
        }

        const auto digit = static_cast<__uint128_t>(ch - '0');
        if (result > (limit - digit) / 10) {
            throw std::out_of_range("out of range __int128_t");
        }
        result = result * 10 + digit;
    }

    if (!negative) {
        return static_cast<__int128_t>(result);
    }
    if (result == kInt128MinAbs) {
        return std::numeric_limits<__int128_t>::min();
    }
    return -static_cast<__int128_t>(result);
}

inline std::string ToString(__int128_t value) {
    if (value == 0) {
        return "0";
    }

    const bool negative = value < 0;
    __uint128_t magnitude = negative
        ? static_cast<__uint128_t>(-(value + 1)) + 1
        : static_cast<__uint128_t>(value);

    std::string result;
    while (magnitude > 0) {
        result.push_back(static_cast<char>('0' + static_cast<int>(magnitude % 10)));
        magnitude /= 10;
    }

    if (negative) {
        result.push_back('-');
    }
    std::reverse(result.begin(), result.end());
    return result;
}

template <typename T>
T ParseNumeric(std::string_view value) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>, "NumericColumn requires numeric type");

    if constexpr (std::is_same_v<T, __int128_t>) {
        return ParseInt128(value);
    } else if constexpr (std::is_integral_v<T>) {
        return static_cast<T>(std::stoll(std::string(value)));
    } else {
        return static_cast<T>(std::stold(std::string(value)));
    }
}

template <typename T>
std::vector<T> ParseNumericValues(const std::vector<std::string>& values) {
    std::vector<T> parsed_values;
    parsed_values.reserve(values.size());
    for (const auto& value : values) {
        parsed_values.push_back(ParseNumeric<T>(value));
    }
    return parsed_values;
}

template <typename T>
void WriteNumericElem(Writer& writer, T value, bool is_last) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>, "NumericColumn requires numeric type");

    if constexpr (std::is_same_v<T, __int128_t>) {
        const std::string as_string = ToString(value);
        writer.WriteElem(as_string, is_last);
    } else if constexpr (std::is_integral_v<T>) {
        writer.WriteElem(static_cast<int64_t>(value), is_last);
    } else {
        const std::string as_string = std::to_string(value);
        writer.WriteElem(as_string, is_last);
    }
}
}

template <typename T>
class NumericColumn : public Column {
public:
    NumericColumn() = default;
    NumericColumn(const std::vector<T>& data_) : data(data_) {}
    explicit NumericColumn(const std::vector<std::string>& values)
        : data(column_detail::ParseNumericValues<T>(values)) {}

    void AddElem(std::string&& value) override {
        data.push_back(column_detail::ParseNumeric<T>(value));
    }

    void Print(Writer& writer) const override {
        writer.BinaryWriteVector(data);
    }

    void Read(Reader& reader) override {
        reader.BinaryReadVector(data);
    }

    void PrintElem(Writer& writer, size_t index, bool is_last) const override {
        if (index >= data.size()) { // not bug, a feature
            return;
        }
        column_detail::WriteNumericElem(writer, data[index], is_last);
    }
    
    std::string GetElemToString(size_t index) const override {
        if (index >= data.size()) {
            throw std::out_of_range("index out of range in GetElemToString");
        }
        if constexpr (std::is_same_v<T, __int128_t>) {
            return column_detail::ToString(data[index]);
        } else {
            return std::to_string(data[index]);
        }
    }

    void Accept(ColumnVisitor& visitor) const override {
        visitor.Visit(*this);
    }

    bool Compare(const std::string& elem, size_t index, CompareSign sign) const override {
        if (sign == CompareSign::IN) {
            const std::string_view elem_view(elem);
            size_t start = 0;
            while (start <= elem_view.size()) {
                const size_t comma_pos = elem_view.find(',', start);
                std::string_view token = comma_pos == std::string_view::npos
                    ? elem_view.substr(start)
                    : elem_view.substr(start, comma_pos - start);

                while (!token.empty() && (token.front() == ' ' || token.front() == '\t' || token.front() == '\n' || token.front() == '\r')) {
                    token.remove_prefix(1);
                }
                while (!token.empty() && (token.back() == ' ' || token.back() == '\t' || token.back() == '\n' || token.back() == '\r')) {
                    token.remove_suffix(1);
                }
                if (token.empty()) {
                    throw std::invalid_argument("IN contains empty value");
                }
                if (data[index] == column_detail::ParseNumeric<T>(token)) {
                    return true;
                }
                if (comma_pos == std::string_view::npos) {
                    break;
                }
                start = comma_pos + 1;
            }
            return false;
        }
        if (sign == CompareSign::LIKE || sign == CompareSign::NOT_LIKE) {
            throw std::invalid_argument("LIKE and NOT_LIKE are supported only for string columns");
        }
        return column_detail::BasicCompare(data[index], column_detail::ParseNumeric<T>(elem), sign);
    }

    std::shared_ptr<Column> CopyFiltered(const std::vector<bool>& banned) const override {
        return std::make_shared<NumericColumn<T>>(column_detail::CopyAllowedValues(data, banned));
    }

    std::shared_ptr<Column> CopyReordered(const std::vector<size_t>& ordered) const override {
        return std::make_shared<NumericColumn<T>>(column_detail::CopyReorderedValues(data, ordered));
    }

    size_t Size() const override {
        return data.size();
    }

    const std::vector<T>& Data() const {
        return data;
    }

    ~NumericColumn() override = default;
private:
    static_assert(concepts::BinarySerializable<T>, "NumericColumn requires binary-serializable type");
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>, "NumericColumn requires numeric type");

    std::vector<T> data;
};

using Int128Column = NumericColumn<__int128_t>;
using Int64Column = NumericColumn<int64_t>;
using Int32Column = NumericColumn<int32_t>;
using Int16Column = NumericColumn<int16_t>;
using Int8Column = NumericColumn<int8_t>;
using DoubleColumn = NumericColumn<double>;

class StrColumn : public Column {
public:
    StrColumn() = default;
    StrColumn(const std::vector<std::string>& data_) : data(data_) {}
    StrColumn(std::vector<std::string>&& data_) : data(std::move(data_)) {}
    StrColumn(const StrColumn& other, const std::vector<bool>& banned);
    void AddElem(std::string&&) override;
    void Print(Writer&) const override;
    void Read(Reader&) override;
    void PrintElem(Writer&, size_t, bool) const override;
    std::string GetElemToString(size_t index) const override;
    void Accept(ColumnVisitor& visitor) const override;
    bool Compare(const std::string&, size_t, CompareSign) const override;
    std::shared_ptr<Column> CopyFiltered(const std::vector<bool>& banned) const override;
    std::shared_ptr<Column> CopyReordered(const std::vector<size_t>& ordered) const override;
    size_t Size() const override;
    const std::vector<std::string>& Data() const;
    ~StrColumn() override = default;
protected:
    std::vector<std::string> data;
};

class TimeStampColumn : public StrColumn {
public:
    TimeStampColumn() = default;
    TimeStampColumn(const std::vector<std::string>& data_) : StrColumn(data_) {}
    TimeStampColumn(std::vector<std::string>&& data_) : StrColumn(std::move(data_)) {}
    TimeStampColumn(const TimeStampColumn& other, const std::vector<bool>& banned) : StrColumn(other, banned) {}
    std::shared_ptr<Column> CopyReordered(const std::vector<size_t>& ordered) const override;
    void Accept(ColumnVisitor& visitor) const override;
    ~TimeStampColumn() override = default;
private:
};

class DateColumn : public StrColumn {
public:
    DateColumn() = default;
    DateColumn(const std::vector<std::string>& data_) : StrColumn(data_) {}
    DateColumn(std::vector<std::string>&& data_) : StrColumn(std::move(data_)) {}
    DateColumn(const DateColumn& other, const std::vector<bool>& banned) : StrColumn(other, banned) {}
    std::shared_ptr<Column> CopyReordered(const std::vector<size_t>& ordered) const override;
    void Accept(ColumnVisitor& visitor) const override;
    ~DateColumn() override = default;
private:
};
