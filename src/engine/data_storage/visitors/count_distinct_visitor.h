#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_set>

#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/visitor.h"

struct CountDistinctVisitor : public ColumnVisitor {
    template <typename ColumnT>
    void IntegralVisit(const ColumnT& col, size_t ind) {
        if (ind == -1u) {
             for (const auto& elem : col.Data()) {
                integral_values.insert(static_cast<int64_t>(elem));
            }
        } else {
            integral_values.insert(static_cast<int64_t>(col.ValueAt(ind)));
        }
    }

    void Visit(const Int8Column& col, size_t ind) override  { IntegralVisit(col, ind); }
    void Visit(const Int16Column& col, size_t ind) override { IntegralVisit(col, ind); }
    void Visit(const Int32Column& col, size_t ind) override { IntegralVisit(col, ind); }
    void Visit(const Int64Column& col, size_t ind) override { IntegralVisit(col, ind); }

    void Visit(const Int128Column& col, size_t ind) override {
        if (ind == -1u) {
            for (const auto& elem : col.Data()) {
                string_values.insert(column_detail::ToString(elem));
            }
        } else {
            string_values.insert(column_detail::ToString(col.ValueAt(ind)));
        }
    }

    void Visit(const DoubleColumn& col, size_t ind) override {
        if (ind == -1u) {
            for (const auto& elem : col.Data()) {
                double_values.insert(elem);
            }
        } else {
            double_values.insert(col.ValueAt(ind));
        }
    }

    void Visit(const StrColumn& col, size_t ind) override {
        if (ind == -1u) {
            for (const auto& elem : col.Data()) {
                string_values.insert(elem);
            }
        } else {
            string_values.insert(col.ValueAt(ind));
        }
    }

    void Visit(const DateColumn& col, size_t ind) override {
        if (ind == -1u) {
            for (const auto& elem : col.Data()) {
                string_values.insert(elem);
            }
        } else {
            string_values.insert(col.ValueAt(ind));
        }
    }

    void Visit(const TimeStampColumn& col, size_t ind) override {
        if (ind == -1u) {
            for (const auto& elem : col.Data()) {
                string_values.insert(elem);
            }
        } else {
            string_values.insert(col.ValueAt(ind));
        }
    }

    size_t Count() const {
        return integral_values.size() + string_values.size() + double_values.size();
    }

private:
    std::unordered_set<int64_t>    integral_values;
    std::unordered_set<double>     double_values;
    std::unordered_set<std::string> string_values;
};
