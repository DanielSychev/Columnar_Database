#pragma once

#include "CsvMfReader/reader.h"
#include "queries_executor/aggregation.h"
#include "utils.h"
#include <cstddef>
#include <istream>
#include <memory>
#include <string>
#include <vector>

struct Transform;

enum class OperatorType {
    SCAN,
    FILTER,
    TRANSFORM,
    AGGREGATION,
    GROUPBY,
    ORDERBY,
    LIMIT,
};

struct Operator {
    virtual ~Operator() = default;

    OperatorType type;
    std::shared_ptr<Operator> child;
};

struct ScanOperator : public Operator {
    ScanOperator(std::istream& data_reader_stream, const std::vector<std::string>& col_names);
    virtual ~ScanOperator() = default;
    
    Reader data_reader;
    std::vector<std::string> column_names;
};

struct FilterOperator : public Operator {
    FilterOperator(std::shared_ptr<Operator> child_op, std::vector<std::string>&& column_names, std::vector<std::string>&& values, std::vector<CompareSign>&& signs);
    virtual ~FilterOperator() = default;
    std::vector<std::string> column_names;
    std::vector<std::string> values;
    std::vector<CompareSign> signs;
};

struct TransformsOperator : public Operator {
    TransformsOperator(std::shared_ptr<Operator> child_op, std::vector<std::shared_ptr<Transform>> transforms_);
    virtual ~TransformsOperator() = default;
    std::vector<std::shared_ptr<Transform>> transforms;
};

struct AggregateOperator : public Operator {
    AggregateOperator(std::shared_ptr<Operator> child_op, std::vector<std::shared_ptr<Aggregation>> aggregations);
    virtual ~AggregateOperator() = default;
    std::vector<std::shared_ptr<Aggregation>> aggs;
};

struct GroupByOperator : public Operator {
    GroupByOperator(std::shared_ptr<Operator> child_op, const std::vector<std::string>& group_by_columns, const std::vector<std::shared_ptr<Aggregation>>& aggs);
    virtual ~GroupByOperator() = default;
    std::vector<std::string> group_by_columns;
    std::vector<std::shared_ptr<Aggregation>> aggs;
};

struct OrderByOperator : public Operator {
    OrderByOperator(std::shared_ptr<Operator> child_op, std::vector<std::string>&& column_names, bool descending = false, size_t limit = Constants::ORDER_BY_LIMIT);
    virtual ~OrderByOperator() = default;
    std::vector<std::string> column_names;
    bool descending;
    size_t limit;
};

struct LimitOperator : public Operator {
    LimitOperator(std::shared_ptr<Operator> child_op, size_t limit);
    virtual ~LimitOperator() = default;
    size_t limit;
};
