#pragma once

#include <stdexcept>
#include <string>
#include <string_view>

#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/visitor.h"


struct DateMinMaxVisitor : public ColumnVisitor {
    // template <typename ColumnT>
    void NumericVisit() {
        throw std::runtime_error("date function for non-date column");
    }

    template <typename ColumnT>
    void DateOrTimeVisit(const ColumnT& col) {
        const auto& data = col.Data();
        for (auto& elem : data) {
            if (max.empty() || elem > max) {
                max = elem;
            }
            if (min.empty() || elem < min) {
                min = elem;
            }
        }
    }

    void Visit(const Int8Column&) override { NumericVisit(); }
    void Visit(const Int16Column&) override { NumericVisit(); }
    void Visit(const Int32Column&) override { NumericVisit(); }
    void Visit(const Int64Column&) override { NumericVisit(); }
    void Visit(const Int128Column&) override { NumericVisit(); }
    void Visit(const DoubleColumn&) override { NumericVisit(); }

    void Visit(const StrColumn&) override {
        throw std::runtime_error("date function for string");
    }

    void Visit(const DateColumn& col) override { DateOrTimeVisit(col); }
    void Visit(const TimeStampColumn& col) override { DateOrTimeVisit(col); }

    std::string_view Max() const {
        if (max.empty()) {
            throw std::runtime_error("max is empty");
        }
        return max;
    }

    std::string_view Min() const {
        if (min.empty()) {
            throw std::runtime_error("min is empty");
        }
        return min;
    }

private:
    std::string max;
    std::string min;
};
