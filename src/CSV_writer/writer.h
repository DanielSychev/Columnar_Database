#pragma once

#include <string>
#include <ostream>
#include <fstream>

class DataStorage;

class Writer {
public:
    // Writer(const std::string& file_path, char delimetr = ',');
    Writer(std::ostream& ss, char delimetr = ',');
    void WriteDataStorage(const DataStorage& data);
    void WriteElem(int64_t x, bool);
    void WriteElem(const std::string& s, bool); // флаг bool = true, если конец ряда
    void CheckFlag(bool fl);
private:
    std::ostream& out_;
    char delimetr_;
};