#pragma once

#include <boost/unordered/unordered_flat_set.hpp>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/visitor.h"
#include "utils.h"

struct CountDistinctVisitor : public ColumnVisitor {

    template <typename ColumnT>
    void IntegralVisit(const ColumnT& col, const std::vector<size_t>& group_indices, size_t max_group_index) {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        visitor_detail::EnsureCapacity(group_integral_values, max_group_index);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            group_integral_values[group_ind].insert(static_cast<int64_t>(data[j]));
        }
    }

    void Visit(const Int8Column& col,   const std::vector<size_t>& gi, size_t mg) override { IntegralVisit(col, gi, mg); }
    void Visit(const Int16Column& col,  const std::vector<size_t>& gi, size_t mg) override { IntegralVisit(col, gi, mg); }
    void Visit(const Int32Column& col,  const std::vector<size_t>& gi, size_t mg) override { IntegralVisit(col, gi, mg); }
    void Visit(const Int64Column& col,  const std::vector<size_t>& gi, size_t mg) override { IntegralVisit(col, gi, mg); }

    void Visit(const Int128Column& col, const std::vector<size_t>& group_indices, size_t max_group_index) override {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        visitor_detail::EnsureCapacity(group_int128_values, max_group_index);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            group_int128_values[group_ind].insert(data[j]);
        }
    }

    void Visit(const DoubleColumn& col, const std::vector<size_t>& group_indices, size_t max_group_index) override {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        visitor_detail::EnsureCapacity(group_double_values, max_group_index);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            group_double_values[group_ind].insert(data[j]);
        }
    }

    void Visit(const StrColumn& col, const std::vector<size_t>& group_indices, size_t max_group_index) override {
        const size_t n = std::min(col.Size(), group_indices.size());
        visitor_detail::EnsureCapacity(group_string_values, max_group_index);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            group_string_values[group_ind].emplace(col.GetElemView(j));
        }
    }

    void Visit(const DateColumn& col, const std::vector<size_t>& group_indices, size_t max_group_index) override {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        visitor_detail::EnsureCapacity(group_integral_values, max_group_index);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            group_integral_values[group_ind].insert(DateToInt32(data[j]));
        }
    }

    void Visit(const TimeStampColumn& col, const std::vector<size_t>& group_indices, size_t max_group_index) override {
        const auto& data = col.Data();
        const size_t n = std::min(data.size(), group_indices.size());
        visitor_detail::EnsureCapacity(group_integral_values, max_group_index);
        for (size_t j = 0; j < n; ++j) {
            size_t group_ind = group_indices[j];
            if (group_ind == SIZE_MAX) continue;
            group_integral_values[group_ind].insert(TimeStampToInt64(data[j]));
        }
    }

    size_t Count(size_t g) const {
        size_t result = 0;
        if (g < group_integral_values.size())  result += group_integral_values[g].size();
        if (g < group_int128_values.size())    result += group_int128_values[g].size();
        if (g < group_double_values.size())    result += group_double_values[g].size();
        if (g < group_string_values.size())    result += group_string_values[g].size();
        return result;
    }

private:
    std::vector<boost::unordered_flat_set<int64_t>>                                    group_integral_values;
    std::vector<boost::unordered_flat_set<__int128_t, visitor_detail::Int128Hash>>     group_int128_values;
    std::vector<boost::unordered_flat_set<double>>                                     group_double_values;
    std::vector<boost::unordered_flat_set<std::string>>                                group_string_values;
};
