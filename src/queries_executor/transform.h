#pragma once

#include "engine/data_storage/batch.h"
#include "engine/data_storage/column.h"
#include "engine/data_storage/schema.h"
#include <memory>
#include <regex>
#include <string>
#include <set>

// Row-preserving transformation that appends a derived column to the batch.
struct Transform {
    explicit Transform(const std::string& result_name_);
    virtual ~Transform() = default;

    virtual Type ResultType(const Schema& input_schema) const = 0;
    virtual std::shared_ptr<Column> Apply(const Batch& batch) const = 0;

    const std::string& GetResultName() const;
protected:
    std::string result_name;
};

struct ExtractMinuteTransform : public Transform {
    ExtractMinuteTransform(const std::string& source_column_name_, const std::string& result_name = "");

    Type ResultType(const Schema& input_schema) const override;
    std::shared_ptr<Column> Apply(const Batch& batch) const override;

private:
    std::string source_column_name;
};

struct DateTruncMinuteTransform : public Transform {
    DateTruncMinuteTransform(const std::string& source_column_name_, const std::string& result_name = "");

    Type ResultType(const Schema& input_schema) const override;
    std::shared_ptr<Column> Apply(const Batch& batch) const override;

private:
    std::string source_column_name;
};

struct LengthTransform : public Transform {
    LengthTransform(const std::string& source_column_name_, const std::string& result_name = "");

    Type ResultType(const Schema& input_schema) const override;
    std::shared_ptr<Column> Apply(const Batch& batch) const override;

private:
    std::string source_column_name;
};

struct RegexpReplaceTransform : public Transform {
    RegexpReplaceTransform(const std::string& source_column_name_, const std::string& pattern_, const std::string& replacement_, const std::string& result_name = "");

    Type ResultType(const Schema& input_schema) const override;
    std::shared_ptr<Column> Apply(const Batch& batch) const override;

private:
    std::string source_column_name;
    std::regex regex_pattern;
    std::string replacement;
};

struct AddTransform : public Transform {
    AddTransform(const std::string& source_column_name_, int64_t value_, const std::string& result_name_ = "");

    Type ResultType(const Schema& input_schema) const override;
    std::shared_ptr<Column> Apply(const Batch& batch) const override;

private:
    std::string source_column_name;
    int64_t value;
    std::set<Type> allowed_types{Type::int64, Type::int32, Type::int16, Type::int8};
};

struct SubTransform : public AddTransform {
    SubTransform(const std::string& source_column_name_, int64_t value_, const std::string& result_name_ = "");
};

struct ConstantTransform : public Transform {
    ConstantTransform(const std::string& value_, const std::string& result_name_ = "");

    Type ResultType(const Schema&) const override;
    std::shared_ptr<Column> Apply(const Batch& batch) const override;

private:
    std::string value;
};

struct RenameTransform : public Transform {
    RenameTransform(const std::string& source_column_name_, const std::string& result_name_ = "");

    Type ResultType(const Schema& input_schema) const override;
    std::shared_ptr<Column> Apply(const Batch& batch) const override;

private:
    std::string source_column_name;
};

struct CaseWhenTransform: public Transform {
    CaseWhenTransform(const std::vector<std::string>& condition_column_names_, const std::vector<std::string>& condition_values_, const std::vector<CompareSign>& condition_signs_, 
        const std::string& column_true_, const std::string& column_false_, const std::string& result_name_ = "");

        Type ResultType(const Schema& input_schema) const override;
        std::shared_ptr<Column> Apply(const Batch& batch) const override;
private:
    std::vector<std::string> condition_column_names;
    std::vector<std::string> condition_values;
    std::vector<CompareSign> condition_signs;
    std::string column_true;
    std::string column_false;
};
