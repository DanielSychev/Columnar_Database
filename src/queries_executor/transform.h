#pragma once

#include "engine/data_storage/batch.h"
#include "engine/data_storage/column.h"
#include "engine/data_storage/schema.h"
#include <memory>
#include <string>

// Row-preserving transformation that appends a derived column to the batch.
struct Transform {
    explicit Transform(std::string result_name_);
    virtual ~Transform() = default;

    virtual Type ResultType(const Schema& input_schema) const = 0;
    virtual std::shared_ptr<Column> Apply(const Batch& batch) const = 0;

    std::string result_name;
};

struct ExtractMinuteTransform : public Transform {
    ExtractMinuteTransform(const std::string& source_column_name_, std::string result_name = "");

    Type ResultType(const Schema& input_schema) const override;
    std::shared_ptr<Column> Apply(const Batch& batch) const override;

    std::string source_column_name;
};
