#pragma once

#include <string>
#include <ostream>
#include <fstream>

namespace concepts_write {
    template<typename T>
    concept BinarySerializable = 
        std::is_integral_v<T> || 
        std::is_same_v<T, float> || 
        std::is_same_v<T, double>;
};

class Writer {
public:
    // Writer(const std::string& file_path, char delimetr = ',');
    Writer(std::ostream& ss, char delimetr = ',');
    void WriteElem(int64_t x, bool);
    void WriteElem(const std::string& s, bool); // флаг bool = true, если конец ряда
    void CheckFlag(bool fl);
    size_t TellPos();

    template<concepts_write::BinarySerializable T>
    void BinaryWrite(const T& value) {
        out_.write(reinterpret_cast<const char*>(&value), sizeof(T));
    }
private:
    std::ostream& out_;
    char delimetr_;
};