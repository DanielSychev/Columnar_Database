#pragma once

#include <stdexcept>
#include <string>
#include <string_view>

#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/visitor.h"


struct DateMinMaxVisitor : public ColumnVisitor {
    void NumericVisit() {
        throw std::runtime_error("date function for non-date column");
    }

    template <typename ColumnT>
    void DateOrTimeVisit(const ColumnT& col, size_t ind) {
        if (ind == -1u) {
            for (const auto& elem : col.Data()) {
                if (max.empty() || elem > max) max = elem;
                if (min.empty() || elem < min) min = elem;
            }
        } else {
            const auto elem = col.ValueAt(ind);
            if (max.empty() || elem > max) max = elem;
            if (min.empty() || elem < min) min = elem;
        }
    }

    void Visit(const Int8Column&, size_t) override { NumericVisit(); }
    void Visit(const Int16Column&, size_t) override { NumericVisit(); }
    void Visit(const Int32Column&, size_t) override { NumericVisit(); }
    void Visit(const Int64Column&, size_t) override { NumericVisit(); }
    void Visit(const Int128Column&, size_t) override { NumericVisit(); }
    void Visit(const DoubleColumn&, size_t) override { NumericVisit(); }

    void Visit(const StrColumn& col, size_t ind) override { DateOrTimeVisit(col, ind); }
    void Visit(const DateColumn& col, size_t ind) override { DateOrTimeVisit(col, ind); }
    void Visit(const TimeStampColumn& col, size_t ind) override { DateOrTimeVisit(col, ind); }

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
