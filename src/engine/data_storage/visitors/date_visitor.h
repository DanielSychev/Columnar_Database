#pragma once

#include <climits>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/visitor.h"

struct StrMaxVisitor : public ColumnVisitor {
    void Visit(const Int8Column&,        const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMaxVisitor: non-str column"); }
    void Visit(const Int16Column&,       const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMaxVisitor: non-str column"); }
    void Visit(const Int32Column&,       const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMaxVisitor: non-str column"); }
    void Visit(const Int64Column&,       const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMaxVisitor: non-str column"); }
    void Visit(const Int128Column&,      const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMaxVisitor: non-str column"); }
    void Visit(const DoubleColumn&,      const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMaxVisitor: non-str column"); }
    void Visit(const DateColumn&,        const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMaxVisitor: non-str column"); }
    void Visit(const TimeStampColumn&,   const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMaxVisitor: non-str column"); }

    void Visit(const StrColumn& col, const std::vector<size_t>& group_indices, size_t max_group_index) override {
        const size_t n = std::min(col.Size(), group_indices.size());
        visitor_detail::EnsureCapacity(group_max_str, max_group_index);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            const auto elem = col.GetElemView(j);
            if (group_max_str[group_ind].empty() || elem > group_max_str[group_ind])
                group_max_str[group_ind] = elem;
        }
    }

    std::string MaxStr(size_t g) const {
        if (g >= group_max_str.size()) throw std::runtime_error("StrMaxVisitor: value not set");
        return group_max_str[g];
    }

private:
    std::vector<std::string> group_max_str;
};


struct StrMinVisitor : public ColumnVisitor {
    void Visit(const Int8Column&,        const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMinVisitor: non-str column"); }
    void Visit(const Int16Column&,       const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMinVisitor: non-str column"); }
    void Visit(const Int32Column&,       const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMinVisitor: non-str column"); }
    void Visit(const Int64Column&,       const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMinVisitor: non-str column"); }
    void Visit(const Int128Column&,      const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMinVisitor: non-str column"); }
    void Visit(const DoubleColumn&,      const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMinVisitor: non-str column"); }
    void Visit(const DateColumn&,        const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMinVisitor: non-str column"); }
    void Visit(const TimeStampColumn&,   const std::vector<size_t>&, size_t) override { throw std::runtime_error("StrMinVisitor: non-str column"); }

    void Visit(const StrColumn& col, const std::vector<size_t>& group_indices, size_t max_group_index) override {
        const size_t n = std::min(col.Size(), group_indices.size());
        visitor_detail::EnsureCapacity(group_min_str, max_group_index);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            const auto elem = col.GetElemView(j);
            if (group_min_str[group_ind].empty() || elem < group_min_str[group_ind])
                group_min_str[group_ind] = elem;
        }
    }

    std::string MinStr(size_t g) const {
        if (g >= group_min_str.size()) throw std::runtime_error("StrMinVisitor: value not set");
        return group_min_str[g];
    }

private:
    std::vector<std::string> group_min_str;
};


struct DateMaxVisitor : public ColumnVisitor {
    void Visit(const Int8Column&,    const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMaxVisitor: non-date column"); }
    void Visit(const Int16Column&,   const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMaxVisitor: non-date column"); }
    void Visit(const Int32Column&,   const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMaxVisitor: non-date column"); }
    void Visit(const Int64Column&,   const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMaxVisitor: non-date column"); }
    void Visit(const Int128Column&,  const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMaxVisitor: non-date column"); }
    void Visit(const DoubleColumn&,  const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMaxVisitor: non-date column"); }
    void Visit(const StrColumn&,     const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMaxVisitor: non-date column"); }

    void Visit(const DateColumn& col, const std::vector<size_t>& group_indices, size_t max_group_index) override {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        visitor_detail::EnsureCapacity(group_max_date, max_group_index, INT32_MIN);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            const int32_t x = DateToInt32(data[j]);
            if (x > group_max_date[group_ind]) group_max_date[group_ind] = x;
        }
    }

    void Visit(const TimeStampColumn& col, const std::vector<size_t>& group_indices, size_t max_group_index) override {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        visitor_detail::EnsureCapacity(group_max_timestamp, max_group_index, INT64_MIN);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            const int64_t x = TimeStampToInt64(data[j]);
            if (x > group_max_timestamp[group_ind]) group_max_timestamp[group_ind] = x;
        }
    }

    int32_t MaxDate(size_t g) const {
        if (g >= group_max_date.size()) return INT32_MIN;
        return group_max_date[g];
    }
    int64_t MaxTimestamp(size_t g) const {
        if (g >= group_max_timestamp.size()) return INT64_MIN;
        return group_max_timestamp[g];
    }

private:
    std::vector<int32_t> group_max_date;
    std::vector<int64_t> group_max_timestamp;
};


struct DateMinVisitor : public ColumnVisitor {
    void Visit(const Int8Column&,    const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMinVisitor: non-date column"); }
    void Visit(const Int16Column&,   const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMinVisitor: non-date column"); }
    void Visit(const Int32Column&,   const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMinVisitor: non-date column"); }
    void Visit(const Int64Column&,   const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMinVisitor: non-date column"); }
    void Visit(const Int128Column&,  const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMinVisitor: non-date column"); }
    void Visit(const DoubleColumn&,  const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMinVisitor: non-date column"); }
    void Visit(const StrColumn&,     const std::vector<size_t>&, size_t) override { throw std::runtime_error("DateMinVisitor: non-date column"); }

    void Visit(const DateColumn& col, const std::vector<size_t>& group_indices, size_t max_group_index) override {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        visitor_detail::EnsureCapacity(group_min_date, max_group_index, INT32_MAX);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            const int32_t x = DateToInt32(data[j]);
            if (x < group_min_date[group_ind]) group_min_date[group_ind] = x;
        }
    }

    void Visit(const TimeStampColumn& col, const std::vector<size_t>& group_indices, size_t max_group_index) override {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        visitor_detail::EnsureCapacity(group_min_timestamp, max_group_index, INT64_MAX);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            const int64_t x = TimeStampToInt64(data[j]);
            if (x < group_min_timestamp[group_ind]) group_min_timestamp[group_ind] = x;
        }
    }

    int32_t MinDate(size_t g) const {
        if (g >= group_min_date.size()) return INT32_MAX;
        return group_min_date[g];
    }
    int64_t MinTimestamp(size_t g) const {
        if (g >= group_min_timestamp.size()) return INT64_MAX;
        return group_min_timestamp[g];
    }

private:
    std::vector<int32_t> group_min_date;
    std::vector<int64_t> group_min_timestamp;
};
