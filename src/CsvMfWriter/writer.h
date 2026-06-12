#pragma once

#include <ostream>
#include <string_view>
#include <vector>
#include <utils.h>

class Writer {
public:
    explicit Writer(std::ostream& ss, char delimetr = ',');
    void WriteNBytes(const char* buffer, size_t n);
    void WriteElem(int64_t x, bool);
    void WriteElem(std::string_view s, bool); // флаг bool = true, если ставим '\n'
    size_t TellPos();

    template<concepts::BinarySerializable T>
    void BinaryWrite(const T& value) {
        out_.write(reinterpret_cast<const char*>(&value), sizeof(T));
    }

    template<concepts::BinarySerializable T>
    void BinaryWriteVector(const std::vector<T>& values) {
        const size_t count = values.size();
        BinaryWrite(count);
        if (!values.empty()) {
            out_.write(
                reinterpret_cast<const char*>(values.data()),
                static_cast<std::streamsize>(values.size() * sizeof(T))
            );
        }
    }
private:
    void CheckFlag(bool fl);
    
    std::ostream& out_;
    char delimetr_;
};
