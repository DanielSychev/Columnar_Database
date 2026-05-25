#pragma once

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/visitor.h"

// ─── NumericSumVisitor ────────────────────────────────────────────────────────
struct NumericSumVisitor : public ColumnVisitor {
    template <typename ColumnT>
    void IntegralVisit(const ColumnT& col, const std::vector<size_t>& group_indices) {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        size_t max_ind = 0;
        for (size_t j = 0; j < n; ++j) {
            if (group_indices[j] != SIZE_MAX && group_indices[j] > max_ind) max_ind = group_indices[j];
        }
        visitor_detail::EnsureCapacity(group_sum_i, max_ind, __int128_t{0});
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            group_sum_i[group_ind] += data[j];
        }
    }

    void Visit(const Int8Column& col,   const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int16Column& col,  const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int32Column& col,  const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int64Column& col,  const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int128Column& col, const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }

    void Visit(const DoubleColumn& col, const std::vector<size_t>& group_indices) override {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        size_t max_ind = 0;
        for (size_t j = 0; j < n; ++j) {
            if (group_indices[j] != SIZE_MAX && group_indices[j] > max_ind) max_ind = group_indices[j];
        }
        visitor_detail::EnsureCapacity(group_sum_d, max_ind, 0.0);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            group_sum_d[group_ind] += data[j];
        }
    }

    void Visit(const StrColumn&,       const std::vector<size_t>&) override { throw std::runtime_error("NumericSumVisitor: str column"); }
    void Visit(const DateColumn&,      const std::vector<size_t>&) override { throw std::runtime_error("NumericSumVisitor: date column"); }
    void Visit(const TimeStampColumn&, const std::vector<size_t>&) override { throw std::runtime_error("NumericSumVisitor: timestamp column"); }

    __int128_t IntegralSum(size_t g) const {
        if (g >= group_sum_i.size()) return 0;
        return group_sum_i[g];
    }
    double DoubleSum(size_t g) const {
        if (g >= group_sum_d.size()) return 0.0;
        return group_sum_d[g];
    }

private:
    std::vector<__int128_t> group_sum_i;
    std::vector<double>     group_sum_d;
};


// ─── NumericAvgVisitor ────────────────────────────────────────────────────────
struct NumericAvgVisitor : public ColumnVisitor {
    template <typename ColumnT>
    void IntegralVisit(const ColumnT& col, const std::vector<size_t>& group_indices) {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        size_t max_ind = 0;
        for (size_t j = 0; j < n; ++j) {
            if (group_indices[j] != SIZE_MAX && group_indices[j] > max_ind) max_ind = group_indices[j];
        }
        visitor_detail::EnsureCapacity(group_sum_i, max_ind, __int128_t{0});
        visitor_detail::EnsureCapacity(group_cnt,   max_ind, int64_t{0});
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            group_sum_i[group_ind] += data[j];
            ++group_cnt[group_ind];
        }
        is_double = false;
    }

    void Visit(const Int8Column& col,   const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int16Column& col,  const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int32Column& col,  const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int64Column& col,  const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int128Column& col, const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }

    void Visit(const DoubleColumn& col, const std::vector<size_t>& group_indices) override {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        size_t max_ind = 0;
        for (size_t j = 0; j < n; ++j) {
            if (group_indices[j] != SIZE_MAX && group_indices[j] > max_ind) max_ind = group_indices[j];
        }
        visitor_detail::EnsureCapacity(group_sum_d, max_ind, 0.0);
        visitor_detail::EnsureCapacity(group_cnt,   max_ind, int64_t{0});
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            group_sum_d[group_ind] += data[j];
            ++group_cnt[group_ind];
        }
        is_double = true;
    }

    void Visit(const StrColumn&,       const std::vector<size_t>&) override { throw std::runtime_error("NumericAvgVisitor: str column"); }
    void Visit(const DateColumn&,      const std::vector<size_t>&) override { throw std::runtime_error("NumericAvgVisitor: date column"); }
    void Visit(const TimeStampColumn&, const std::vector<size_t>&) override { throw std::runtime_error("NumericAvgVisitor: timestamp column"); }

    int64_t Avg(size_t g) const {
        if (g >= group_cnt.size() || group_cnt[g] == 0) return 0;
        if (is_double) return static_cast<int64_t>(group_sum_d[g] / static_cast<double>(group_cnt[g]));
        return static_cast<int64_t>(group_sum_i[g] / static_cast<__int128_t>(group_cnt[g]));
    }

    bool is_double = false;
private:
    std::vector<__int128_t> group_sum_i;
    std::vector<double>     group_sum_d;
    std::vector<int64_t>    group_cnt;
};


// ─── NumericMaxVisitor ────────────────────────────────────────────────────────
struct NumericMaxVisitor : public ColumnVisitor {
    template <typename ColumnT>
    void IntegralVisit(const ColumnT& col, const std::vector<size_t>& group_indices) {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        size_t max_ind = 0;
        for (size_t j = 0; j < n; ++j) {
            if (group_indices[j] != SIZE_MAX && group_indices[j] > max_ind) max_ind = group_indices[j];
        }
        visitor_detail::EnsureCapacity(group_max_i, max_ind, std::numeric_limits<int64_t>::lowest());
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            const int64_t elem = static_cast<int64_t>(data[j]);
            if (elem > group_max_i[group_ind]) group_max_i[group_ind] = elem;
        }
        is_double = false;
    }

    void Visit(const Int8Column& col,   const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int16Column& col,  const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int32Column& col,  const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int64Column& col,  const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int128Column& col, const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }

    void Visit(const DoubleColumn& col, const std::vector<size_t>& group_indices) override {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        size_t max_ind = 0;
        for (size_t j = 0; j < n; ++j) {
            if (group_indices[j] != SIZE_MAX && group_indices[j] > max_ind) max_ind = group_indices[j];
        }
        visitor_detail::EnsureCapacity(group_max_d, max_ind, std::numeric_limits<double>::lowest());
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            if (data[j] > group_max_d[group_ind]) group_max_d[group_ind] = data[j];
        }
        is_double = true;
    }

    void Visit(const StrColumn&,       const std::vector<size_t>&) override { throw std::runtime_error("NumericMaxVisitor: str column"); }
    void Visit(const DateColumn&,      const std::vector<size_t>&) override { throw std::runtime_error("NumericMaxVisitor: date column"); }
    void Visit(const TimeStampColumn&, const std::vector<size_t>&) override { throw std::runtime_error("NumericMaxVisitor: timestamp column"); }

    int64_t Max(size_t g) const {
        if (is_double) {
            if (g >= group_max_d.size()) return std::numeric_limits<int64_t>::lowest();
            return static_cast<int64_t>(group_max_d[g]);
        }
        if (g >= group_max_i.size()) return std::numeric_limits<int64_t>::lowest();
        return group_max_i[g];
    }

    bool is_double = false;
private:
    std::vector<int64_t> group_max_i;
    std::vector<double>  group_max_d;
};


// ─── NumericMinVisitor ────────────────────────────────────────────────────────
struct NumericMinVisitor : public ColumnVisitor {
    template <typename ColumnT>
    void IntegralVisit(const ColumnT& col, const std::vector<size_t>& group_indices) {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        size_t max_ind = 0;
        for (size_t j = 0; j < n; ++j) {
            if (group_indices[j] != SIZE_MAX && group_indices[j] > max_ind) max_ind = group_indices[j];
        }
        visitor_detail::EnsureCapacity(group_min_i, max_ind, std::numeric_limits<int64_t>::max());
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            const int64_t elem = static_cast<int64_t>(data[j]);
            if (elem < group_min_i[group_ind]) group_min_i[group_ind] = elem;
        }
        is_double = false;
    }

    void Visit(const Int8Column& col,   const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int16Column& col,  const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int32Column& col,  const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int64Column& col,  const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }
    void Visit(const Int128Column& col, const std::vector<size_t>& gi) override { IntegralVisit(col, gi); }

    void Visit(const DoubleColumn& col, const std::vector<size_t>& group_indices) override {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        size_t max_ind = 0;
        for (size_t j = 0; j < n; ++j) {
            if (group_indices[j] != SIZE_MAX && group_indices[j] > max_ind) max_ind = group_indices[j];
        }
        visitor_detail::EnsureCapacity(group_min_d, max_ind, std::numeric_limits<double>::max());
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            if (data[j] < group_min_d[group_ind]) group_min_d[group_ind] = data[j];
        }
        is_double = true;
    }

    void Visit(const StrColumn&,       const std::vector<size_t>&) override { throw std::runtime_error("NumericMinVisitor: str column"); }
    void Visit(const DateColumn&,      const std::vector<size_t>&) override { throw std::runtime_error("NumericMinVisitor: date column"); }
    void Visit(const TimeStampColumn&, const std::vector<size_t>&) override { throw std::runtime_error("NumericMinVisitor: timestamp column"); }

    int64_t Min(size_t g) const {
        if (is_double) {
            if (g >= group_min_d.size()) return std::numeric_limits<int64_t>::max();
            return static_cast<int64_t>(group_min_d[g]);
        }
        if (g >= group_min_i.size()) return std::numeric_limits<int64_t>::max();
        return group_min_i[g];
    }

    bool is_double = false;
private:
    std::vector<int64_t> group_min_i;
    std::vector<double>  group_min_d;
};
