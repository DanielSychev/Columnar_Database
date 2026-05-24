#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>

#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/visitor.h"


struct DateMinMaxVisitor : public ColumnVisitor {
    void NumericVisit() {
        throw std::runtime_error("date function for non-date column");
    }

    void StrVisit(const StrColumn& col, size_t ind) {
        if (ind == -1u) {
            for (const auto& elem : col.Data()) {
                if (max_str.empty() || elem > max_str) max_str = elem;
                if (min_str.empty() || elem < min_str) min_str = elem;
            }
        } else {
            const auto elem = col.ValueAt(ind);
            if (max_str.empty() || elem > max_str) max_str = elem;
            if (min_str.empty() || elem < min_str) min_str = elem;
        }
    }

    void DateVisit(const DateColumn& col, size_t ind) {
        if (ind == -1u) {
            for (const auto& elem : col.Data()) {
                int32_t x = DateToInt32(elem);
                if (x > max_date) max_date = x;
                if (x < min_date) min_date = x;
            }
        } else {
            const auto elem = col.ValueAt(ind);
            int32_t x = DateToInt32(elem);
            if (x > max_date) max_date = x;
            if (x < min_date) min_date = x;
        }
    }

    void TimeStampVisit(const TimeStampColumn& col, size_t ind) {
        if (ind == -1u) {
            for (const auto& elem : col.Data()) {
                int64_t x = TimeStampToInt64(elem);
                if (x > max_timestamp) max_timestamp = x;
                if (x < min_timestamp) min_timestamp = x;
            }
        } else {
            const auto elem = col.ValueAt(ind);
            int64_t x = TimeStampToInt64(elem);
            if (x > max_timestamp) max_timestamp = x;
            if (x < min_timestamp) min_timestamp = x;
        }
    }


    void Visit(const Int8Column&, size_t) override { NumericVisit(); }
    void Visit(const Int16Column&, size_t) override { NumericVisit(); }
    void Visit(const Int32Column&, size_t) override { NumericVisit(); }
    void Visit(const Int64Column&, size_t) override { NumericVisit(); }
    void Visit(const Int128Column&, size_t) override { NumericVisit(); }
    void Visit(const DoubleColumn&, size_t) override { NumericVisit(); }

    void Visit(const StrColumn& col, size_t ind) override { StrVisit(col, ind); }
    void Visit(const DateColumn& col, size_t ind) override { DateVisit(col, ind); }
    void Visit(const TimeStampColumn& col, size_t ind) override { TimeStampVisit(col, ind); }

    const std::string& MaxStr() const {
        if (max_str.empty()) throw std::runtime_error("max date value is not set");
        return max_str;
    }
    const std::string& MinStr() const {
        if (min_str.empty()) throw std::runtime_error("min date value is not set");
        return min_str;
    }
    int32_t MaxDate() const { return max_date; }
    int32_t MinDate() const { return min_date; }
    int64_t MaxTimestamp() const { return max_timestamp; }
    int64_t MinTimestamp() const { return min_timestamp; }

private:
    std::string max_str;
    std::string min_str;
    int32_t max_date = INT32_MIN, min_date = INT32_MAX;
    int64_t max_timestamp = INT64_MIN, min_timestamp = INT64_MAX;
};
