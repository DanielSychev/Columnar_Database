#pragma once

#include <vector>
#include <CSV_writer/writer.h>

class Column {
public:
    virtual void AddElem(std::string&&) = 0; // чтобы move делать
    virtual void Print(Writer&) = 0;
    virtual ~Column() = default;
private:

};

class Int64Column : public Column {
public:
    void AddElem(std::string&&) override;
    void Print(Writer&) override;
    ~Int64Column() override = default;
private:
    std::vector<int64_t> data;
};

class StrColumn : public Column {
public:
    void AddElem(std::string&&) override;
    void Print(Writer&) override;
    ~StrColumn() override = default;
private:
    std::vector<std::string> data;
};