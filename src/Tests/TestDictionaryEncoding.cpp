#include <gtest/gtest.h>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "CsvMfReader/reader.h"
#include "CsvMfWriter/writer.h"
#include "engine/data_storage/compression/dictionary_encoding.h"

namespace {
std::vector<std::string> RoundTrip(const std::vector<std::string>& input) {
    std::vector<std::string_view> views(input.begin(), input.end());

    std::stringstream stream;
    Writer writer(stream);
    dictionary_encoding::Encode(writer, views);

    Reader reader(stream);
    std::vector<char> buf;
    std::vector<size_t> offsets;
    size_t count = 0;
    dictionary_encoding::Decode(reader, buf, offsets, count);

    std::vector<std::string> out;
    out.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        out.emplace_back(buf.data() + offsets[i], offsets[i + 1] - offsets[i]);
    }
    return out;
}
}

TEST(DictionaryEncoding, LowCardinalityUsesDictionary) {
    std::vector<std::string> input{"red", "green", "red", "blue", "green", "red"};
    EXPECT_EQ(RoundTrip(input), input);
}

TEST(DictionaryEncoding, HighCardinalityFallsBackToRaw) {
    std::vector<std::string> input{"a", "b", "c", "d", "e"};
    EXPECT_EQ(RoundTrip(input), input);
}

TEST(DictionaryEncoding, EmptyStringsPreserved) {
    std::vector<std::string> input{"", "x", "", "x", ""};
    EXPECT_EQ(RoundTrip(input), input);
}

TEST(DictionaryEncoding, SingleValue) {
    std::vector<std::string> input{"only"};
    EXPECT_EQ(RoundTrip(input), input);
}

TEST(DictionaryEncoding, AllSame) {
    std::vector<std::string> input{"same", "same", "same", "same"};
    EXPECT_EQ(RoundTrip(input), input);
}

TEST(DictionaryEncoding, LongStrings) {
    std::vector<std::string> input{
        "http://example.com/very/long/url/path?with=query&and=params",
        "http://example.com/very/long/url/path?with=query&and=params",
        "http://another.com/page",
    };
    EXPECT_EQ(RoundTrip(input), input);
}

TEST(DictionaryEncoding, PreservesOrderWithMixedRepeats) {
    std::vector<std::string> input{"b", "a", "b", "b", "a", "c", "a"};
    EXPECT_EQ(RoundTrip(input), input);
}
