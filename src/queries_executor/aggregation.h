#pragma once

#include "engine/data_storage/batch.h"
#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/sum_visitor.h"
#include <memory>
#include <optional>
// #include <queries_executor/operator.h>

struct Aggregation {
    virtual void RunBatch(std::shared_ptr<Batch>) = 0;
    virtual std::shared_ptr<Column> GetResult() const = 0;
    virtual ~Aggregation() = default;
};

struct CountAggregation : public Aggregation {
    void RunBatch(std::shared_ptr<Batch>) override;
    virtual ~CountAggregation() = default;
    std::shared_ptr<Column> GetResult() const override;

    size_t rows_count = 0;
};

struct SumAggregation : public Aggregation {
    SumAggregation(const std::string col_name);
    void RunBatch(std::shared_ptr<Batch>) override;
    std::shared_ptr<Column> GetResult() const override;
    virtual ~SumAggregation() = default;

    std::string column_name;
    SumVisitor visitor;
    std::optional<Type> input_type;
};

struct AvgAggregation : public Aggregation {
    AvgAggregation(const std::string col_name);
    void RunBatch(std::shared_ptr<Batch>) override;
    std::shared_ptr<Column> GetResult() const override;
    virtual ~AvgAggregation() = default;

    std::string column_name;
    SumVisitor visitor;
    std::optional<Type> input_type;
};


// struct AvgAggregation : public Aggregation {

// };
