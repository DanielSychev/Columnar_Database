#pragma once

#include <cstdint>

template <typename T>
class NumericColumn;

using Int128Column = NumericColumn<__int128_t>;
using Int64Column = NumericColumn<int64_t>;
using Int32Column = NumericColumn<int32_t>;
using Int16Column = NumericColumn<int16_t>;
using Int8Column = NumericColumn<int8_t>;
using DoubleColumn = NumericColumn<double>;

class StrColumn;
class DateColumn;
class TimeStampColumn;

struct ColumnVisitor {
    virtual void Visit(const Int128Column& column) = 0;
    virtual void Visit(const Int64Column& column) = 0;
    virtual void Visit(const Int32Column& column) = 0;
    virtual void Visit(const Int16Column& column) = 0;
    virtual void Visit(const Int8Column& column) = 0;
    virtual void Visit(const DoubleColumn& column) = 0;
    virtual void Visit(const StrColumn& column) = 0;
    virtual void Visit(const DateColumn& column) = 0;
    virtual void Visit(const TimeStampColumn& column) = 0;
    virtual ~ColumnVisitor() = default;
};
