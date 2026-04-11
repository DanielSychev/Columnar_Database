#pragma once

#include "CsvMfReader/reader.h"
#include <istream>
#include <memory>

enum class OperatorType {
    SCAN,
    COUNT,
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

struct CountOperator : public Operator {
    CountOperator(std::shared_ptr<Operator> child_op);
    virtual ~CountOperator() = default;
};
