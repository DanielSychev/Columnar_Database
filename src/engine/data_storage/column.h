#pragma once

#include "engine/data_storage/schema.h"
#include <functional>
#include <stdexcept>
#include <vector>
#include "CsvMfWriter/writer.h"

// template <typename T>
// void PrintElem(Writer& w, const std::vector<T>& v, size_t ind) {
//     w.WriteElem(v[ind]);
// }

class Column {
public:
    virtual void AddElem(std::string&&) = 0; // чтобы move делать
    virtual void Print(Writer&) const = 0;
    virtual void PrintElem(Writer&, size_t, bool) const = 0;
    virtual size_t Size() const = 0;
    virtual ~Column() = default;

    Type column_type;
private:
};

class Int64Column : public Column {
public:
    void AddElem(std::string&&) override;
    void Print(Writer&) const override;
    void PrintElem(Writer&, size_t, bool) const override;
    size_t Size() const override;
    ~Int64Column() override = default;
private:
    std::vector<int64_t> data;
};

class StrColumn : public Column {
public:
    void AddElem(std::string&&) override;
    void Print(Writer&) const override;
    void PrintElem(Writer&, size_t, bool) const override;
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
