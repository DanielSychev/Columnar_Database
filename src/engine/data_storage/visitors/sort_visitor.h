#pragma once

#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/visitor.h"
#include <numeric>

struct SortVisitor : ColumnVisitor {
    explicit SortVisitor(size_t n, bool desc) : order(n), descending(desc) {
        std::iota(order.begin(), order.end(), 0);
    }

    template <class ColumnT>
    void SortBy(const ColumnT& col) {
        const auto& data = col.Data();
        std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
            return descending ? data[a] > data[b] : data[a] < data[b];
        });
    }

    void Visit(const Int8Column& col) override { SortBy(col); }
    void Visit(const Int16Column& col) override { SortBy(col); }
    void Visit(const Int32Column& col) override { SortBy(col); }
    void Visit(const Int64Column& col) override { SortBy(col); }
    void Visit(const Int128Column& col) override { SortBy(col); }
    void Visit(const DoubleColumn& col) override { SortBy(col); }
    void Visit(const StrColumn& c) override { SortBy(c); }
    void Visit(const DateColumn& c) override { SortBy(c); }
    void Visit(const TimeStampColumn& c) override { SortBy(c); }

    std::vector<size_t> order;
    bool descending;
};
