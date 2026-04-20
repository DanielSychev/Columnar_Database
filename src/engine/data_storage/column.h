#pragma once

// #include "engine/data_storage/schema.h"
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"
#include "engine/data_storage/visitors/visitor.h"

// template <typename T>
// void PrintElem(Writer& w, const std::vector<T>& v, size_t ind) {
//     w.WriteElem(v[ind]);
// }

class Column {
public:
    virtual void AddElem(std::string&&) = 0; // чтобы move делать
    virtual void Print(Writer&) const = 0;
    virtual void Read(Reader&) = 0;
    virtual void PrintElem(Writer&, size_t, bool) const = 0;
    virtual void Accept(ColumnVisitor& visitor) const = 0;
    virtual bool Compare(const std::string&, size_t) const = 0;
    virtual std::shared_ptr<Column> CopyFiltered(const std::vector<bool>& banned) const = 0;
    virtual size_t Size() const = 0;
    virtual ~Column() = default;
};

namespace column_detail {
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
T ParseNumeric(std::string_view value) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>, "NumericColumn requires numeric type");

    if constexpr (std::is_integral_v<T>) {
        return static_cast<T>(std::stoll(std::string(value)));
    } else {
        return static_cast<T>(std::stold(std::string(value)));
    }
}

template <typename T>
void WriteNumericElem(Writer& writer, T value, bool is_last) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>, "NumericColumn requires numeric type");

    if constexpr (std::is_integral_v<T>) {
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
    NumericColumn(const NumericColumn& other, const std::vector<bool>& banned) :
    data(column_detail::CopyAllowedValues(other.data, banned)) {}

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

    void Accept(ColumnVisitor& visitor) const override {
        visitor.Visit(*this);
    }

    bool Compare(const std::string& elem, size_t index) const override {
        return data[index] == column_detail::ParseNumeric<T>(elem);
    }

    std::shared_ptr<Column> CopyFiltered(const std::vector<bool>& banned) const override {
        return std::make_shared<NumericColumn<T>>(*this, banned);
    }

    size_t Size() const override {
        return data.size();
    }

    const std::vector<T>& Data() const {
        return data;
    }

    ~NumericColumn() override = default;
private:
    static_assert(concepts_read::BinarySerializable<T>, "NumericColumn requires binary-serializable type");
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>, "NumericColumn requires numeric type");

    std::vector<T> data;
};

using Int64Column = NumericColumn<int64_t>;
using Int32Column = NumericColumn<int32_t>;
using Int16Column = NumericColumn<int16_t>;
using Int8Column = NumericColumn<int8_t>;
using DoubleColumn = NumericColumn<double>;

class StrColumn : public Column {
public:
    StrColumn() = default;
    StrColumn(const StrColumn& other, const std::vector<bool>& banned);
    void AddElem(std::string&&) override;
    void Print(Writer&) const override;
    void Read(Reader&) override;
    void PrintElem(Writer&, size_t, bool) const override;
    void Accept(ColumnVisitor& visitor) const override;
    bool Compare(const std::string&, size_t) const override;
    std::shared_ptr<Column> CopyFiltered(const std::vector<bool>& banned) const override;
    size_t Size() const override;
    ~StrColumn() override = default;
private:
    std::vector<std::string> data;
};

// template <
// void Execute(Column& c, ) {
//     switch (c.column_type) {
//         case Type::int64:

//             break;
//         case Type::str:
//             break;
//         default:
//             throw std::runtime_error("another type");
//     }
// }
