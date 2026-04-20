#pragma once

#include "CsvMfReader/reader.h"
#include "queries_executor/aggregation.h"
#include <istream>
#include <memory>

enum class OperatorType {
    SCAN,
    // COUNT,
    FILTER,
    PROJECTION,
    JOIN,
    AGGREGATION,
};

struct Operator {
    virtual ~Operator() = default;

    OperatorType type;
    std::shared_ptr<Operator> child;
};

// дальше различные операторы

struct ScanOperator : public Operator {
    ScanOperator(std::istream& data_reader_stream, const std::vector<std::string>& col_names);
    virtual ~ScanOperator() = default;
    
    Reader data_reader;
    std::vector<std::string> column_names;
};

// struct CountOperator : public Operator {
//     CountOperator(std::shared_ptr<Operator> child_op);
//     virtual ~CountOperator() = default;
// };

struct FilterOperator : public Operator {
    FilterOperator(std::shared_ptr<Operator> child_op, const std::string& column_name_, std::string&& value_);
    virtual ~FilterOperator() = default;
    std::string column_name;
    std::string value;
};

struct AggregateOperator : public Operator {
    AggregateOperator(std::shared_ptr<Operator> child_op, std::vector<std::shared_ptr<Aggregation>> aggregations);
    virtual ~AggregateOperator() = default;
    std::vector<std::shared_ptr<Aggregation>> aggs;
};
