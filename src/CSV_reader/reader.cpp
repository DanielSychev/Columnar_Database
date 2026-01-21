#include "CSV_reader/reader.h"
#include <iostream>

// Reader::Reader(const std::string& filename, char delimetr) : delimetr_(delimetr) {
//     file_ = new std::ifstream(filename);
// }

Reader::Reader(std::istream& ss, char delimetr) : file_(ss), delimetr_(delimetr) {
}

bool Reader::ReadLine(std::vector<std::string>& result) {
    result.clear();
    char c;
    bool in_quotes = false, any_read = false;;
    std::string cur_string = "";
    while (file_.get(c)) {
        any_read = true;
        if (c == '"') {
            if (in_quotes){
                if (file_.peek() == '"') {
                    cur_string += '"';
                    file_.get();
                } else {
                    in_quotes = false;
                }
            } else {
                in_quotes = true;
            }
        } else if (in_quotes) {
            cur_string += c;
        } else {
            if (c == '\n') {
                break;
            } else if (c == delimetr_) {
                result.push_back(cur_string);
                cur_string = "";
            } else {
                cur_string += c;
            }
        }
    }
    if (!any_read) {
        return false;
    }
    if (in_quotes) {
        std::cerr << "Bad csv was given, not given closing quote";
        return false;
    }
    result.push_back(cur_string);
    return true;
}

bool Reader::ReadRows(std::vector<std::vector<std::string>>& rows, size_t n) {
    if (rows.size() != n) {
        throw std::runtime_error("rows size is not enough to hold all rows");
    }
    for (size_t i = 0; i < n; ++i) {
        if (!ReadLine(rows[i])) {
            if (i == 0) { // ничего не прочитали
                return false;
            }
            break;
        }
    }
    return true;
}