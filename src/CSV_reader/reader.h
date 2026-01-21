#pragma once

#include <vector>
#include <string>

class Reader {
public:
    Reader(std::istream& ss, char delimetr = ',');

    bool ReadLine(std::vector<std::string>& result);

    bool ReadRows(std::vector<std::vector<std::string>>& rows, size_t n); // bool = false обозначает ничего не прочитали
private:
    std::istream& file_;
    char delimetr_;
};