#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace visitor_detail {

template<typename T>
inline void EnsureCapacity(std::vector<T>& vec, size_t g, const T& default_val = T{}) {
    if (g >= vec.size()) vec.resize(g + 1, default_val);
}

struct Int128Hash {
    size_t operator()(__int128_t val) const {
        const uint64_t lo = static_cast<uint64_t>(val);
        const uint64_t hi = static_cast<uint64_t>(static_cast<__uint128_t>(val) >> 64);
        return std::hash<uint64_t>{}(lo) ^ (std::hash<uint64_t>{}(hi) << 1);
    }
};

} // namespace visitor_detail

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
    virtual void Visit(const Int128Column& column, const std::vector<uint32_t>& group_indices, size_t max_group_index) = 0;
    virtual void Visit(const Int64Column& column, const std::vector<uint32_t>& group_indices, size_t max_group_index) = 0;
    virtual void Visit(const Int32Column& column, const std::vector<uint32_t>& group_indices, size_t max_group_index) = 0;
    virtual void Visit(const Int16Column& column, const std::vector<uint32_t>& group_indices, size_t max_group_index) = 0;
    virtual void Visit(const Int8Column& column, const std::vector<uint32_t>& group_indices, size_t max_group_index) = 0;
    virtual void Visit(const DoubleColumn& column, const std::vector<uint32_t>& group_indices, size_t max_group_index) = 0;
    virtual void Visit(const StrColumn& column, const std::vector<uint32_t>& group_indices, size_t max_group_index) = 0;
    virtual void Visit(const DateColumn& column, const std::vector<uint32_t>& group_indices, size_t max_group_index) = 0;
    virtual void Visit(const TimeStampColumn& column, const std::vector<uint32_t>& group_indices, size_t max_group_index) = 0;

    virtual ~ColumnVisitor() = default;
};
