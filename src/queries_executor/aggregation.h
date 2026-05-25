#pragma once

#include "engine/data_storage/batch.h"
#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/numeric_visitor.h"
#include "engine/data_storage/visitors/date_visitor.h"
#include "engine/data_storage/visitors/count_distinct_visitor.h"
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

struct Aggregation {
    Aggregation(std::string column_name_, std::string result_name_ = "") : column_name(column_name_), result_name(result_name_) {}
    virtual void RunBatch(std::shared_ptr<Batch>, std::vector<size_t> group_indices) = 0;
    // virtual void RunRow(std::shared_ptr<Batch>, size_t) = 0;
    virtual std::string GetResultValue(size_t group_index) const = 0;
    virtual Type GetResultType() const = 0;
    virtual std::shared_ptr<Aggregation> Clone() const = 0;
    virtual ~Aggregation() = default;

    std::string column_name;
    std::string result_name;
    std::optional<Type> input_type;
};

struct CountAggregation : public Aggregation {
    CountAggregation(std::string result_name_ = "");
    void RunBatch(std::shared_ptr<Batch>, std::vector<size_t> group_indices) override;
    // void RunRow(std::shared_ptr<Batch>, size_t) override;
    std::string GetResultValue(size_t group_index) const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~CountAggregation() = default;

    std::vector<size_t> rows_count;
};

struct SumAggregation : public Aggregation {
    SumAggregation(const std::string col_name, std::string result_name = "");
    void RunBatch(std::shared_ptr<Batch>, std::vector<size_t> group_indices) override;
    // void RunRow(std::shared_ptr<Batch>, size_t) override;
    std::string GetResultValue(size_t group_index) const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~SumAggregation() = default;

    NumericSumVisitor visitor;
};

struct AvgAggregation : public Aggregation {
    AvgAggregation(const std::string col_name, std::string result_name = "");
    void RunBatch(std::shared_ptr<Batch>, std::vector<size_t> group_indices) override;
    // void RunRow(std::shared_ptr<Batch>, size_t) override;
    std::string GetResultValue(size_t group_index) const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~AvgAggregation() = default;

    NumericAvgVisitor visitor;
};

struct CountDistinctAggregation : public Aggregation {
    CountDistinctAggregation(const std::string col_name, std::string result_name = "");
    void RunBatch(std::shared_ptr<Batch>, std::vector<size_t> group_indices) override;
    // void RunRow(std::shared_ptr<Batch>, size_t) override;
    std::string GetResultValue(size_t group_index) const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~CountDistinctAggregation() = default;

    CountDistinctVisitor visitor;
};

struct MaxAggregation : public Aggregation {
    MaxAggregation(const std::string col_name, std::string result_name = "");
    void RunBatch(std::shared_ptr<Batch>, std::vector<size_t> group_indices) override;
    // void RunRow(std::shared_ptr<Batch>, size_t) override;
    std::string GetResultValue(size_t group_index) const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~MaxAggregation() = default;

    NumericMaxVisitor numeric_visitor;
    DateMaxVisitor date_visitor;
    StrMaxVisitor str_visitor;
};

struct MinAggregation : public Aggregation {
    MinAggregation(const std::string col_name, std::string result_name = "");
    void RunBatch(std::shared_ptr<Batch>, std::vector<size_t> group_indices) override;
    // void RunRow(std::shared_ptr<Batch>, size_t) override;
    std::string GetResultValue(size_t group_index) const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~MinAggregation() = default;

    NumericMinVisitor numeric_visitor;
    DateMinVisitor date_visitor;
    StrMinVisitor str_visitor;
};
