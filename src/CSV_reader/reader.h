#pragma once

#include <vector>
#include <string>
#include <fstream>
#include <sstream>

class Reader {
public:
    // Reader(const std::string& filename, char delimetr = ',');

    Reader(std::istream& ss, char delimetr = ',');

    bool ReadLine(std::vector<std::string>& result);

private:
    std::istream& file_;
    char delimetr_;
};