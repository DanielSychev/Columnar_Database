#pragma once

#include "engine/data_storage/batch.h"
#include "engine/data_storage/column.h"
#include "engine/data_storage/schema.h"
#include <memory>
#include <regex>
#include <string>

// Row-preserving transformation that appends a derived column to the batch.
struct Transform {
    explicit Transform(const std::string& result_name_);
    virtual ~Transform() = default;

    virtual Type ResultType(const Schema& input_schema) const = 0;
    virtual std::shared_ptr<Column> Apply(const Batch& batch) const = 0;

    std::string result_name;
};

struct ExtractMinuteTransform : public Transform {
    ExtractMinuteTransform(const std::string& source_column_name_, const std::string& result_name = "");

    Type ResultType(const Schema& input_schema) const override;
    std::shared_ptr<Column> Apply(const Batch& batch) const override;

    std::string source_column_name;
};

struct LengthTransform : public Transform {
    LengthTransform(const std::string& source_column_name_, const std::string& result_name = "");

    Type ResultType(const Schema& input_schema) const override;
    std::shared_ptr<Column> Apply(const Batch& batch) const override;

    std::string source_column_name;
};

struct RegexpReplaceTransform : public Transform {
    RegexpReplaceTransform(const std::string& source_column_name_, const std::string& pattern_, const std::string& replacement_, const std::string& result_name = "");

    Type ResultType(const Schema& input_schema) const override;
    std::shared_ptr<Column> Apply(const Batch& batch) const override;

    std::string source_column_name;
    std::regex regex_pattern;
    std::string replacement;
};
