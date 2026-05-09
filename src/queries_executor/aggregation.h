#pragma once

#include "engine/data_storage/batch.h"
#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/numeric_visitor.h"
#include "engine/data_storage/visitors/date_visitor.h"
#include <memory>
#include <optional>
#include <unordered_set>
// #include <queries_executor/operator.h>

struct Aggregation {
    Aggregation(std::string result_name_ = "") : result_name(result_name_) {}
    virtual void RunBatch(std::shared_ptr<Batch>) = 0;
    virtual std::string GetResultValue() const = 0;
    virtual Type GetResultType() const = 0;
    virtual std::shared_ptr<Aggregation> Clone() const = 0;
    virtual ~Aggregation() = default;

    std::string result_name;
};

struct CountAggregation : public Aggregation {
    CountAggregation(std::string result_name_ = "");
    void RunBatch(std::shared_ptr<Batch>) override;
    virtual ~CountAggregation() = default;
    std::string GetResultValue() const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;

    size_t rows_count = 0;

};

struct SumAggregation : public Aggregation {
    SumAggregation(const std::string col_name, std::string result_name = "");
    void RunBatch(std::shared_ptr<Batch>) override;
    std::string GetResultValue() const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~SumAggregation() = default;

    std::string column_name;
    NumericFuncVisitor visitor;
    std::optional<Type> input_type;
};

struct AvgAggregation : public Aggregation {
    AvgAggregation(const std::string col_name, std::string result_name = "");
    void RunBatch(std::shared_ptr<Batch>) override;
    std::string GetResultValue() const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~AvgAggregation() = default;

    std::string column_name;
    NumericFuncVisitor visitor;
    std::optional<Type> input_type;
};

struct CountDistinctAggregation : public Aggregation {
    CountDistinctAggregation(const std::string col_name, std::string result_name = "");
    void RunBatch(std::shared_ptr<Batch>) override;
    std::string GetResultValue() const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~CountDistinctAggregation() = default;

    std::string column_name;
    std::unordered_set<std::string> distinct_values;
};

struct MaxAggregation : public Aggregation {
    MaxAggregation(const std::string col_name, std::string result_name = "");
    void RunBatch(std::shared_ptr<Batch>) override;
    std::string GetResultValue() const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~MaxAggregation() = default;

    std::string column_name;
    NumericFuncVisitor numeric_visitor;
    DateMinMaxVisitor date_visitor;
    std::optional<Type> input_type;
};

struct MinAggregation : public Aggregation {
    MinAggregation(const std::string col_name, std::string result_name = "");
    void RunBatch(std::shared_ptr<Batch>) override;
    std::string GetResultValue() const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~MinAggregation() = default;

    std::string column_name;
    NumericFuncVisitor numeric_visitor;
    DateMinMaxVisitor date_visitor;
    std::optional<Type> input_type;
};
