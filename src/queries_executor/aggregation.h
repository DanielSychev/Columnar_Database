#pragma once

#include "engine/data_storage/batch.h"
#include "engine/data_storage/column.h"
#include "engine/data_storage/visitors/numeric_visitor.h"
#include "engine/data_storage/visitors/date_visitor.h"
#include "engine/data_storage/visitors/count_distinct_visitor.h"
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

struct Aggregation {
    Aggregation(std::string column_name_, std::string result_name_ = "")
        : column_name(std::move(column_name_)), result_name(std::move(result_name_)) {}
    virtual void RunBatch(const std::shared_ptr<Batch>&, const std::vector<uint32_t>& group_indices, size_t max_group_index) = 0;
    virtual std::string GetResultValue(size_t group_index) const = 0;
    virtual void GetResultInto(Column& column, size_t group_index) const = 0;
    virtual Type GetResultType() const = 0;
    virtual std::shared_ptr<Aggregation> Clone() const = 0;
    virtual ~Aggregation() = default;

    std::string column_name;
    std::string result_name;
    std::optional<Type> input_type;
    size_t column_index_cache = SIZE_MAX;
};

struct CountAggregation : public Aggregation {
    CountAggregation(std::string result_name_ = "");
    void RunBatch(const std::shared_ptr<Batch>&, const std::vector<uint32_t>& group_indices, size_t max_group_index) override;
    std::string GetResultValue(size_t group_index) const override;
    void GetResultInto(Column& column, size_t group_index) const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~CountAggregation() = default;

    std::vector<size_t> rows_count;
};

struct SumAggregation : public Aggregation {
    SumAggregation(std::string col_name, std::string result_name = "");
    void RunBatch(const std::shared_ptr<Batch>&, const std::vector<uint32_t>& group_indices, size_t max_group_index) override;
    std::string GetResultValue(size_t group_index) const override;
    void GetResultInto(Column& column, size_t group_index) const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~SumAggregation() = default;

    NumericSumVisitor visitor;
};

struct AvgAggregation : public Aggregation {
    AvgAggregation(std::string col_name, std::string result_name = "");
    void RunBatch(const std::shared_ptr<Batch>&, const std::vector<uint32_t>& group_indices, size_t max_group_index) override;
    std::string GetResultValue(size_t group_index) const override;
    void GetResultInto(Column& column, size_t group_index) const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~AvgAggregation() = default;

    NumericAvgVisitor visitor;
};

struct CountDistinctAggregation : public Aggregation {
    CountDistinctAggregation(std::string col_name, std::string result_name = "");
    void RunBatch(const std::shared_ptr<Batch>&, const std::vector<uint32_t>& group_indices, size_t max_group_index) override;
    std::string GetResultValue(size_t group_index) const override;
    void GetResultInto(Column& column, size_t group_index) const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~CountDistinctAggregation() = default;

    CountDistinctVisitor visitor;
};

struct MaxAggregation : public Aggregation {
    MaxAggregation(std::string col_name, std::string result_name = "");
    void RunBatch(const std::shared_ptr<Batch>&, const std::vector<uint32_t>& group_indices, size_t max_group_index) override;
    std::string GetResultValue(size_t group_index) const override;
    void GetResultInto(Column& column, size_t group_index) const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~MaxAggregation() = default;

    NumericMaxVisitor numeric_visitor;
    DateMaxVisitor date_visitor;
    StrMaxVisitor str_visitor;
};

struct MinAggregation : public Aggregation {
    MinAggregation(std::string col_name, std::string result_name = "");
    void RunBatch(const std::shared_ptr<Batch>&, const std::vector<uint32_t>& group_indices, size_t max_group_index) override;
    std::string GetResultValue(size_t group_index) const override;
    void GetResultInto(Column& column, size_t group_index) const override;
    Type GetResultType() const override;
    std::shared_ptr<Aggregation> Clone() const override;
    virtual ~MinAggregation() = default;

    NumericMinVisitor numeric_visitor;
    DateMinVisitor date_visitor;
    StrMinVisitor str_visitor;
};
