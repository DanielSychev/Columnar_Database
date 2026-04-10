#pragma once

#include <vector>
#include <string>
#include <istream>

namespace concepts_read {
    template<typename T>
    concept BinarySerializable = 
        std::is_integral_v<T> || 
        std::is_same_v<T, float> || 
        std::is_same_v<T, double>;
};

class Reader {
public:
    explicit Reader(std::istream& ss, char delimetr = ',');

    bool ReadLine(std::vector<std::string>& result);

    bool ReadRows(std::vector<std::vector<std::string>>& rows, size_t n); // bool = false обозначает ничего не прочитали

    void SetPos(size_t pos);

    size_t ReadLastBytes();

    template<concepts_read::BinarySerializable T>
    void BinaryRead(T& value) {
        file_.read(reinterpret_cast<char*>(&value), sizeof(T));
    }

    template<concepts_read::BinarySerializable T>
    void BinaryReadVector(std::vector<T>& values) {
        size_t count = 0;
        BinaryRead(count);
        values.resize(count);
        if (!values.empty()) {
            file_.read(
                reinterpret_cast<char*>(values.data()),
                static_cast<std::streamsize>(values.size() * sizeof(T))
            );
        }
    }
private:
    std::istream& file_;
    char delimetr_;
};
