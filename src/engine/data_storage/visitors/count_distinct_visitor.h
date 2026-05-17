#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>

#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/visitor.h"

struct CountDistinctVisitor : public ColumnVisitor {
    template <typename ColumnT>
    void IntegralVisit(const ColumnT& col) {
        for (const auto& elem : col.Data()) {
            integral_values.insert(static_cast<int64_t>(elem));
        }
    }

    void Visit(const Int8Column& col) override  { IntegralVisit(col); }
    void Visit(const Int16Column& col) override { IntegralVisit(col); }
    void Visit(const Int32Column& col) override { IntegralVisit(col); }
    void Visit(const Int64Column& col) override { IntegralVisit(col); }

    void Visit(const Int128Column& col) override {
        for (const auto& elem : col.Data()) {
            string_values.insert(column_detail::ToString(elem));
        }
    }

    void Visit(const DoubleColumn& col) override {
        for (const auto& elem : col.Data()) {
            double_values.insert(elem);
        }
    }

    void Visit(const StrColumn& col) override {
        for (const auto& elem : col.Data()) {
            string_values.insert(elem);
        }
    }

    void Visit(const DateColumn& col) override {
        for (const auto& elem : col.Data()) {
            string_values.insert(elem);
        }
    }

    void Visit(const TimeStampColumn& col) override {
        for (const auto& elem : col.Data()) {
            string_values.insert(elem);
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
