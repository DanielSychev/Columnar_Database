#pragma once

#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"
#include "utils.h"
#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace dictionary_encoding {

namespace transform_helper {
inline void WriteRaw(Writer& writer, const std::vector<std::string_view>& data) {
    writer.BinaryWrite(Tag::RAW);
    writer.BinaryWrite(data.size());
    for (const auto& str : data) {
        writer.BinaryWrite(str.size());
        writer.WriteNBytes(str.data(), str.size());
    }
}

inline void ReadStringInto(Reader& reader, std::vector<char>& buf, std::vector<size_t>& offsets) {
    size_t str_size;
    reader.BinaryRead(str_size);
    size_t old_size = buf.size();
    buf.resize(old_size + str_size);
    if (str_size > 0 && !reader.ReadNBytes(buf.data() + old_size, str_size)) {
        throw std::runtime_error("not enough bytes to read string");
    }
    offsets.push_back(buf.size());
}
}

inline void Encode(Writer& writer, const std::vector<std::string_view>& data) {
    std::unordered_map<std::string_view, uint32_t> dict;
    std::vector<uint32_t> indices;
    indices.reserve(data.size());
    uint32_t cnt = 0;
    bool use_dict = true;
    for (const auto& str : data) {
        auto it = dict.find(str);
        if (it != dict.end()) {
            indices.push_back(it->second);
            continue;
        }
        if ((dict.size() + 1) * 2 > data.size()) { // словарь может не окупиться
            use_dict = false;
            break;
        }
        dict.emplace(str, cnt);
        indices.push_back(cnt++);
    }

    if (!use_dict) {
        transform_helper::WriteRaw(writer, data);
        return;
    }
    writer.BinaryWrite(Tag::DIC_ENCODED);
    std::vector<std::pair<uint32_t, std::string_view>> sorted_keys;
    sorted_keys.reserve(dict.size());
    for (const auto& [key, value] : dict) {
        sorted_keys.push_back({value, key});
    }
    std::sort(sorted_keys.begin(), sorted_keys.end());

    writer.BinaryWrite(dict.size());
    for (const auto& [_, str] : sorted_keys) {
        writer.BinaryWrite(str.size());
        writer.WriteNBytes(str.data(), str.size());
    }
    writer.BinaryWriteVector(indices);
}

inline void Decode(Reader& reader, std::vector<char>& buf, std::vector<size_t>& offsets, size_t& count) {
    Tag tag;
    reader.BinaryRead(tag);
    buf.clear();
    offsets.clear();
    offsets.push_back(0);
    if (tag == Tag::RAW) {
        reader.BinaryRead(count);
        offsets.reserve(count + 1);
        for (size_t i = 0; i < count; ++i) {
            transform_helper::ReadStringInto(reader, buf, offsets);
        }
        return;
    }

    size_t dict_size;
    reader.BinaryRead(dict_size);
    std::vector<char> dict_buf;
    std::vector<size_t> dict_offsets;
    dict_offsets.reserve(dict_size + 1);
    dict_offsets.push_back(0);
    for (size_t i = 0; i < dict_size; ++i) {
        transform_helper::ReadStringInto(reader, dict_buf, dict_offsets);
    }

    std::vector<uint32_t> indices;
    reader.BinaryReadVector(indices);
    count = indices.size();
    size_t buf_size = 0;
    for (uint32_t idx : indices) {
        buf_size += dict_offsets[idx + 1] - dict_offsets[idx];
    }
    buf.reserve(buf_size);
    offsets.reserve(count + 1);

    for (uint32_t idx : indices) {
        const char* src = dict_buf.data() + dict_offsets[idx];
        const size_t len = dict_offsets[idx + 1] - dict_offsets[idx];
        buf.insert(buf.end(), src, src + len);
        offsets.push_back(buf.size());
    }
}
}
