#pragma once

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <type_traits>

#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/visitor.h"

struct NumericFuncVisitor : public ColumnVisitor {
    template <typename ColumnT>
    void SumVisit(const ColumnT& column) {
        const auto& data = column.Data();
        using ValueType = typename std::decay_t<decltype(data)>::value_type;

        if constexpr (std::is_integral_v<ValueType>) {
            for (const auto& elem : data) {
                sum_i += elem;
                ++cnt;
                max_i = std::max<int64_t>(max_i, elem);
                min_i = std::min<int64_t>(min_i, elem);
            }
            result_is_double = false;
        } else {
            for (const auto& elem : data) {
                sum_d += elem;
                ++cnt;
                max_d = std::max<double>(max_d, elem);
                min_d = std::min<double>(min_d, elem);
            }
            result_is_double = true;
        }
    }

    void Visit(const Int8Column& col) override { SumVisit(col); }
    void Visit(const Int16Column& col) override { SumVisit(col); }
    void Visit(const Int32Column& col) override { SumVisit(col); }
    void Visit(const Int64Column& col) override { SumVisit(col); }
    void Visit(const Int128Column& col) override { SumVisit(col); }
    void Visit(const DoubleColumn& col) override { SumVisit(col); }

    void Visit(const StrColumn&) override {
        throw std::runtime_error("numeric function for string (in numeric visitor)");
    }

    void Visit(const DateColumn&) override {
        throw std::runtime_error("numeric function for date (in numeric visitor)");
    }

    void Visit(const TimeStampColumn&) override {
        throw std::runtime_error("numeric function for timestamp (in numeric visitor)");
    }


    __int128_t IntegralSum() const {
        return sum_i;
    }

    double DoubleSum() const {
        return sum_d;
    }

    int64_t Avg() const {
        if (cnt == 0) {
            return 0; // or throw an exception
        }
        // return result_is_double ? sum_d / cnt : static_cast<double>(sum_i) / cnt;
        return sum_i / cnt;
    }

    int64_t Max() const {
        return max_i;
    }

    int64_t Min() const {
        return min_i;
    }

    // bool ResultIsDouble() const {
    //     return result_is_double;
    // }

    __int128_t sum_i = 0;
    int64_t max_i = std::numeric_limits<int64_t>::lowest();
    int64_t min_i = std::numeric_limits<int64_t>::max();
    double sum_d = 0;
    double max_d = std::numeric_limits<double>::lowest();
    double min_d = std::numeric_limits<double>::max();
    int64_t cnt = 0;
    bool result_is_double = false;
};
