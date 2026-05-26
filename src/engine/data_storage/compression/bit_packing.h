#pragma once

#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"
#include <algorithm>
#include <cstdint>
#include <type_traits>
#include <vector>

namespace bit_packing {
template <typename T>
constexpr bool Supported() {
    return std::is_same_v<T, int8_t> ||
           std::is_same_v<T, int16_t> ||
           std::is_same_v<T, int32_t> ||
           std::is_same_v<T, int64_t>;
}

namespace transform_helper {
inline uint8_t BitsNeeded(uint64_t range) {
    if (range == 0) return 0;
    return static_cast<uint8_t>(64 - __builtin_clzll(range));
}

template <typename U>
std::vector<uint64_t> Pack(const std::vector<U>& deltas, uint8_t bits) {
    if (bits == 0 || deltas.empty()) return {};

    const size_t total_bits = deltas.size() * bits;
    const size_t num_words = (total_bits + 63) / 64;
    std::vector<uint64_t> words(num_words, 0ull);
    for (size_t i = 0; i < deltas.size(); ++i) {
        uint64_t val = static_cast<uint64_t>(deltas[i]);
        size_t bit_pos = i * bits;
        size_t word_idx = bit_pos / 64;
        size_t offset = bit_pos % 64;
        words[word_idx] |= val << offset;
        if (offset + bits > 64) {
            words[word_idx + 1] |= val >> (64 - offset);
        }
    }
    return words;
}

template <typename U>
void Unpack(const std::vector<uint64_t>& words, size_t count, uint8_t bits, std::vector<U>& out) {
    out.resize(count);
    if (bits == 0) {
        std::fill(out.begin(), out.end(), U{0});
        return;
    }
    const uint64_t mask = (1ull << bits) - 1;
    for (size_t i = 0; i < count; ++i) {
        size_t bit_pos = i * bits;
        size_t word_idx = bit_pos / 64;
        size_t offset = bit_pos % 64;
        uint64_t val = (words[word_idx] >> offset) & mask;
        if (offset + bits > 64) {
            val |= (words[word_idx + 1] << (64 - offset)) & mask;
        }
        out[i] = static_cast<U>(val);
    }
}
}

template <typename T>
void Encode(Writer& writer, const std::vector<T>& data) {
    if constexpr (!Supported<T>()) {
        writer.BinaryWrite(Tag::RAW);
        writer.BinaryWriteVector(data);
    } else {
        using U = std::make_unsigned_t<T>;
        constexpr uint8_t type_bits = sizeof(T) * 8;

        if (data.empty()) {
            writer.BinaryWrite(Tag::RAW);
            writer.BinaryWriteVector(data);
            return;
        }
        T min_val = *std::min_element(data.begin(), data.end());
        T max_val = *std::max_element(data.begin(), data.end());
        uint64_t range = static_cast<U>(max_val) - static_cast<U>(min_val);
        uint8_t bits = transform_helper::BitsNeeded(range);

        if (bits >= type_bits) {
            writer.BinaryWrite(Tag::RAW);
            writer.BinaryWriteVector(data);
            return;
        }
        writer.BinaryWrite(Tag::BIT_PACKED);
        writer.BinaryWrite(data.size());
        writer.BinaryWrite(min_val);
        writer.BinaryWrite(bits);

        std::vector<U> deltas;
        deltas.reserve(data.size());
        for (const T& v : data) {
            deltas.push_back(static_cast<U>(v) - static_cast<U>(min_val));
        }
        writer.BinaryWriteVector(transform_helper::Pack(deltas, bits));
    }
}

template <typename T>
void Decode(Reader& reader, std::vector<T>& data) {
    Tag tag;
    reader.BinaryRead(tag);

    if (tag == Tag::RAW) {
        reader.BinaryReadVector(data);
        return;
    }

    if constexpr (Supported<T>()) {
        using U = std::make_unsigned_t<T>;
        size_t count;
        reader.BinaryRead(count);
        T min_val;
        reader.BinaryRead(min_val);
        uint8_t bits;
        reader.BinaryRead(bits);
        std::vector<uint64_t> words;
        reader.BinaryReadVector(words);
        std::vector<U> deltas;
        transform_helper::Unpack(words, count, bits, deltas);

        data.resize(count);
        for (size_t i = 0; i < count; ++i) {
            data[i] = static_cast<T>(static_cast<U>(min_val) + deltas[i]);
        }
    }
}
}