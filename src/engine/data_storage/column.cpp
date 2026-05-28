#include "engine/data_storage/column.h"
#include "engine/data_storage/compression/bit_packing.h"
#include "engine/data_storage/compression/dictionary_encoding.h"
#include "utils.h"
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

template <typename T>
void PrintVisitor(Writer& w, const std::vector<T>& v) {
    size_t size = v.size();
    for (size_t i = 0; i < size; ++i) {
        w.WriteElem(v[i], i == size - 1);
    }
}

template <typename T>
void PrintElemVisitor(Writer& w, const std::vector<T>& v, size_t i, bool b) {
    if (i >= v.size()) { // not bug, a feature
        return;
    }
    w.WriteElem(v[i], b);
}

namespace {
bool LikeCompare(std::string_view value, std::string_view pattern) {
    if (pattern.find('_') == std::string_view::npos) {
        const size_t first_pct = pattern.find('%');

        if (first_pct == std::string_view::npos) {
            return value == pattern;
        }

        const size_t last_pct  = pattern.rfind('%');
        const std::string_view prefix = pattern.substr(0, first_pct);
        const std::string_view suffix = pattern.substr(last_pct + 1);
        const std::string_view inner  = pattern.substr(first_pct + 1, last_pct - first_pct - 1);

        if (inner.find('%') == std::string_view::npos) {
            if (!value.starts_with(prefix)) return false;
            if (!value.ends_with(suffix))   return false;
            if (value.size() < prefix.size() + suffix.size()) return false;

            if (first_pct == last_pct) {
                return true;
            }

            if (inner.empty()) return true;
            const std::string_view mid = value.substr(
                prefix.size(),
                value.size() - prefix.size() - suffix.size()
            );
            return mid.find(inner) != std::string_view::npos;
        }
    }

    std::vector<bool> previous(pattern.size() + 1, false);
    std::vector<bool> current(pattern.size() + 1, false);
    previous[0] = true;

    for (size_t pattern_pos = 1; pattern_pos <= pattern.size(); ++pattern_pos) {
        if (pattern[pattern_pos - 1] == '%') {
            previous[pattern_pos] = previous[pattern_pos - 1];
        }
    }

    for (size_t value_pos = 1; value_pos <= value.size(); ++value_pos) {
        current[0] = false;
        for (size_t pattern_pos = 1; pattern_pos <= pattern.size(); ++pattern_pos) {
            if (pattern[pattern_pos - 1] == '%') {
                current[pattern_pos] = current[pattern_pos - 1] || previous[pattern_pos];
            } else if (pattern[pattern_pos - 1] == '_' || pattern[pattern_pos - 1] == value[value_pos - 1]) {
                current[pattern_pos] = previous[pattern_pos - 1];
            } else {
                current[pattern_pos] = false;
            }
        }
        std::swap(previous, current);
        std::fill(current.begin(), current.end(), false);
    }

    return previous[pattern.size()];
}
}

std::shared_ptr<Column> CreateColumn(Type type) {
    switch (type) {
    case Type::int128:
        return std::make_shared<Int128Column>();
    case Type::int64:
        return std::make_shared<Int64Column>();
    case Type::int32:
        return std::make_shared<Int32Column>();
    case Type::int16:
        return std::make_shared<Int16Column>();
    case Type::int8:
        return std::make_shared<Int8Column>();
    case Type::double_:
        return std::make_shared<DoubleColumn>();
    case Type::str:
        return std::make_shared<StrColumn>();
    case Type::date:
        return std::make_shared<DateColumn>();
    case Type::timestamp:
        return std::make_shared<TimeStampColumn>();
    default:
        throw std::runtime_error("unknown type was given (in CreateColumn)");
    }
}

std::shared_ptr<Column> CreateColumn(Type type, const std::vector<std::string>& values) {
    switch (type) {
    case Type::int128:
        return std::make_shared<Int128Column>(values);
    case Type::int64:
        return std::make_shared<Int64Column>(values);
    case Type::int32:
        return std::make_shared<Int32Column>(values);
    case Type::int16:
        return std::make_shared<Int16Column>(values);
    case Type::int8:
        return std::make_shared<Int8Column>(values);
    case Type::double_:
        return std::make_shared<DoubleColumn>(values);
    case Type::str:
        return std::make_shared<StrColumn>(values);
    case Type::date:
        return std::make_shared<DateColumn>(values);
    case Type::timestamp:
        return std::make_shared<TimeStampColumn>(values);
    default:
        throw std::runtime_error("unknown type was given (in CreateColumn)");
    }
}

StrColumn::StrColumn(const std::vector<std::string>& data_) : count(data_.size()) {
    offsets.reserve(count + 1);
    for (const auto& s : data_) {
        buf.insert(buf.end(), s.begin(), s.end());
        offsets.push_back(buf.size());
    }
}

StrColumn::StrColumn(std::vector<std::string>&& data_) : StrColumn(data_) {}

StrColumn::StrColumn(std::vector<char>&& buf_, std::vector<size_t>&& offsets_, size_t count_)
    : buf(std::move(buf_)), offsets(std::move(offsets_)), count(count_) {}

StrColumn::StrColumn(const StrColumn& other, const std::vector<bool>& banned) {
    for (size_t i = 0; i < other.count; ++i) {
        if (i < banned.size() && banned[i]) continue;
        const auto sv = other.GetElemView(i);
        buf.insert(buf.end(), sv.begin(), sv.end());
        offsets.push_back(buf.size());
        ++count;
    }
}

void StrColumn::AppendStr(std::string&& s) {
    buf.insert(buf.end(), s.begin(), s.end());
    offsets.push_back(buf.size());
    ++count;
}

void StrColumn::PrintMf(Writer& w) const {
    std::vector<std::string_view> strings(count);
    for (size_t i = 0; i < count; ++i) {
        strings[i] = GetElemView(i);
    }
    dictionary_encoding::Encode(w, strings);
}

void StrColumn::ReadMf(Reader& r) {
    dictionary_encoding::Decode(r, buf, offsets, count);
}

void StrColumn::PrintElemCsv(Writer& w, size_t i, bool b) const {
    if (i >= count) return;
    w.WriteElem(GetElemView(i), b);
}

std::string StrColumn::GetElemToString(size_t index) const {
    if (index >= count) {
        throw std::out_of_range("index out of range in GetElemToString");
    }
    return std::string(GetElemView(index));
}

std::string_view StrColumn::GetElemView(size_t index) const {
    return { buf.data() + offsets[index], offsets[index + 1] - offsets[index] };
}

void StrColumn::Accept(ColumnVisitor& visitor, const std::vector<size_t>& group_indices) const {
    visitor.Visit(*this, group_indices);
}

bool StrColumn::Compare(const std::string& elem, size_t i, CompareSign sign) const {
    if (sign == CompareSign::IN) {
        throw std::runtime_error("IN sign is not supported for StrColumn");
    }
    const auto sv = GetElemView(i);
    if (sign == CompareSign::LIKE) {
        return LikeCompare(sv, elem);
    }
    if (sign == CompareSign::NOT_LIKE) {
        return !LikeCompare(sv, elem);
    }
    return column_detail::BasicCompare(sv, std::string_view(elem), sign);
}

void StrColumn::Filter(const std::string& value, CompareSign sign, std::vector<bool>& banned) const {
    if (sign == CompareSign::IN) {
        throw std::runtime_error("IN sign is not supported for StrColumn");
    }
    const std::string_view pattern(value);
    if (sign == CompareSign::LIKE) {
        for (size_t i = 0; i < count; ++i) {
            if (banned[i]) continue;
            if (!LikeCompare(GetElemView(i), pattern)) banned[i] = true;
        }
        return;
    }
    if (sign == CompareSign::NOT_LIKE) {
        for (size_t i = 0; i < count; ++i) {
            if (banned[i]) continue;
            if (LikeCompare(GetElemView(i), pattern)) banned[i] = true;
        }
        return;
    }
    for (size_t i = 0; i < count; ++i) {
        if (banned[i]) continue;
        if (!column_detail::BasicCompare(GetElemView(i), pattern, sign)) {
            banned[i] = true;
        }
    }
}

std::shared_ptr<Column> StrColumn::CopyFiltered(const std::vector<bool>& banned) const {
    return std::make_shared<StrColumn>(*this, banned);
}

std::shared_ptr<Column> StrColumn::CopyReordered(const std::vector<size_t>& ordered) const {
    std::vector<char> new_buf;
    std::vector<size_t> new_offsets;
    new_offsets.reserve(ordered.size() + 1);
    new_offsets.push_back(0);
    for (size_t idx : ordered) {
        const auto sv = GetElemView(idx);
        new_buf.insert(new_buf.end(), sv.begin(), sv.end());
        new_offsets.push_back(new_buf.size());
    }
    return std::make_shared<StrColumn>(std::move(new_buf), std::move(new_offsets), ordered.size());
}

size_t StrColumn::Size() const {
    return count;
}

void StrColumn::BinaryWriteInBuf(std::vector<char>& out, size_t index) const {
    const std::string_view sv = GetElemView(index);
    const uint32_t len = static_cast<uint32_t>(sv.size());
    const char* len_ptr = reinterpret_cast<const char*>(&len);
    out.insert(out.end(), len_ptr, len_ptr + sizeof(len));
    out.insert(out.end(), sv.data(), sv.data() + sv.size());
}

void StrColumn::BinaryReadFromBuf(const char*& ptr) {
    uint32_t len;
    std::memcpy(&len, ptr, sizeof(len));
    ptr += sizeof(len);
    buf.insert(buf.end(), ptr, ptr + len);
    offsets.push_back(buf.size());
    ++count;
    ptr += len;
}

void StrColumn::AppendRaw(std::string_view sv) {
    buf.insert(buf.end(), sv.begin(), sv.end());
    offsets.push_back(buf.size());
    ++count;
}

std::string StrColumn::ValueAt(size_t index) const {
    if (index >= count) {
        throw std::out_of_range("index out of range in ValueAt");
    }
    return std::string(GetElemView(index));
}

int StrColumn::CompareAt(size_t index, const Column& other, size_t other_index) const {
    const auto& o = static_cast<const StrColumn&>(other);
    const int raw = GetElemView(index).compare(o.GetElemView(other_index));
    return (raw > 0) - (raw < 0);
}

void StrColumn::AppendFrom(const Column& other, size_t index) {
    const auto& o = static_cast<const StrColumn&>(other);
    AppendRaw(o.GetElemView(index));
}

void DateColumn::AppendStr(std::string&& s) {
    data.push_back(StringToDate(s));
}

DateColumn::DateColumn(const std::vector<std::string>& values) {
    data.reserve(values.size());
    for (const auto& s: values) {
        data.push_back(StringToDate(s));
    }
}

void DateColumn::PrintMf(Writer& writer) const {
    std::vector<int32_t> ints;
    ints.reserve(data.size());
    for (const auto& d : data) {
        ints.push_back(DateToInt32(d));
    }
    bit_packing::Encode(writer, ints);
}

void DateColumn::ReadMf(Reader& reader) {
    std::vector<int32_t> ints;
    bit_packing::Decode(reader, ints);
    data.resize(ints.size());
    for (size_t i = 0; i < ints.size(); ++i) {
        data[i] = Int32ToDate(ints[i]);
    }
}

void DateColumn::PrintElemCsv(Writer& w, size_t i, bool b) const {
    if (i >= data.size()) {
        return;
    }
    return w.WriteElem(DateToString(data[i]), b);
}

std::string DateColumn::GetElemToString(size_t index) const {
    if (index >= data.size()) {
        throw std::out_of_range("index out of range in GetElemToString");
    }
    return DateToString(data[index]);
}

void DateColumn::Accept(ColumnVisitor& visitor, const std::vector<size_t>& group_indices) const {
    visitor.Visit(*this, group_indices);
}

bool DateColumn::Compare(const std::string& elem, size_t i, CompareSign sign) const {
    if (sign == CompareSign::IN) {
        throw std::invalid_argument("IN sign is not supported for DateColumn");
    }
    if (sign == CompareSign::LIKE || sign == CompareSign::NOT_LIKE) {
        throw std::invalid_argument("LIKE and NOT_LIKE are supported only for string columns");
    }
    Date d2 = StringToDate(elem);
    return column_detail::BasicCompare(DateToInt32(data[i]), DateToInt32(d2), sign);
}

void DateColumn::Filter(const std::string& value, CompareSign sign, std::vector<bool>& banned) const {
    if (sign == CompareSign::IN) {
        throw std::invalid_argument("IN sign is not supported for DateColumn");
    }
    if (sign == CompareSign::LIKE || sign == CompareSign::NOT_LIKE) {
        throw std::invalid_argument("LIKE and NOT_LIKE are supported only for string columns");
    }
    const int32_t parsed = DateToInt32(StringToDate(value));
    for (size_t i = 0; i < data.size(); ++i) {
        if (banned[i]) continue;
        if (!column_detail::BasicCompare(DateToInt32(data[i]), parsed, sign)) {
            banned[i] = true;
        }
    }
}

std::shared_ptr<Column> DateColumn::CopyFiltered(const std::vector<bool>& banned) const {
    return std::make_shared<DateColumn>(column_detail::CopyAllowedValues(data, banned));
}

std::shared_ptr<Column> DateColumn::CopyReordered(const std::vector<size_t>& ordered) const {
    return std::make_shared<DateColumn>(column_detail::CopyReorderedValues(data, ordered));
}

size_t DateColumn::Size() const {
    return data.size();
}

void DateColumn::BinaryWriteInBuf(std::vector<char>& out, size_t index) const {
    if (index >= data.size()) {
        throw std::out_of_range("index out of range in BinaryWriteInBuf");
    }
    const int32_t v = DateToInt32(data[index]);
    const char* ptr = reinterpret_cast<const char*>(&v);
    out.insert(out.end(), ptr, ptr + sizeof(v));
}

void DateColumn::BinaryReadFromBuf(const char*& ptr) {
    int32_t v;
    std::memcpy(&v, ptr, sizeof(v));
    ptr += sizeof(v);
    data.push_back(Int32ToDate(v));
}

void DateColumn::AppendRaw(Date date) {
    data.push_back(date);
}

const std::vector<Date>& DateColumn::Data() const {
    return data;
}

Date DateColumn::ValueAt(size_t index) const {
    if (index >= data.size()) {
        throw std::out_of_range("index out of range in ValueAt");
    }
    return data[index];
}

int DateColumn::CompareAt(size_t index, const Column& other, size_t other_index) const {
    const auto& o = static_cast<const DateColumn&>(other);
    int32_t d1 = DateToInt32(data[index]);
    int32_t d2 = DateToInt32(o.data[other_index]);
    return (d1 > d2) - (d1 < d2);
}

void DateColumn::AppendFrom(const Column& other, size_t index) {
    const auto& o = static_cast<const DateColumn&>(other);
    AppendRaw(o.ValueAt(index));
}



std::string DateToString(const Date& date) {
    return std::to_string(date.year) + "-" +
           (date.month < 10 ? "0" : "") + std::to_string(date.month) + "-" +
           (date.day < 10 ? "0" : "") + std::to_string(date.day);
}

Date StringToDate(const std::string& s) {
    if (s.size() != 10 || s[4] != '-' || s[7] != '-') {
        throw std::invalid_argument("wrong date format");
    }
    Date date;
    try {
        date.year = std::stoi(s.substr(0, 4));
        date.month = std::stoi(s.substr(5, 2));
        date.day = std::stoi(s.substr(8, 2));
    } catch (const std::exception& e) {
        throw std::invalid_argument("wrong date format");
    }
    if (date.month < 1 || date.month > 12) {
        throw std::invalid_argument("wrong month value in date");
    }
    if (date.day < 1 || date.day > 31) {
        throw std::invalid_argument("wrong day value in date");
    }
    return date;
}

std::string TimeStampToString(const TimeStamp& timestamp) {
    return DateToString(timestamp) + " " +
           (timestamp.h < 10 ? "0" : "") + std::to_string(timestamp.h) + ":" +
           (timestamp.m < 10 ? "0" : "") + std::to_string(timestamp.m) + ":" +
           (timestamp.s < 10 ? "0" : "") + std::to_string(timestamp.s);
}

TimeStamp StringToTimeStamp(const std::string& s) {
    if (s.size() != 19 || s[4] != '-' || s[7] != '-' || s[10] != ' ' || s[13] != ':' || s[16] != ':') {
        throw std::invalid_argument("wrong timestamp format");
    }
    TimeStamp timestamp;
    try {
        timestamp.year = std::stoi(s.substr(0, 4));
        timestamp.month = std::stoi(s.substr(5, 2));
        timestamp.day = std::stoi(s.substr(8, 2));
        timestamp.h = std::stoi(s.substr(11, 2));
        timestamp.m = std::stoi(s.substr(14, 2));
        timestamp.s = std::stoi(s.substr(17, 2));
    } catch (const std::exception& e) {
        throw std::invalid_argument("wrong timestamp format");
    }
    if (timestamp.month < 1 || timestamp.month > 12) {
        throw std::invalid_argument("wrong month value in timestamp");
    }
    if (timestamp.day < 1 || timestamp.day > 31) {
        throw std::invalid_argument("wrong day value in timestamp");
    }
    if (timestamp.h < 0 || timestamp.h > 23) {
        throw std::invalid_argument("wrong hour value in timestamp");
    }
    if (timestamp.m < 0 || timestamp.m > 59) {
        throw std::invalid_argument("wrong minute value in timestamp");
    }
    if (timestamp.s < 0 || timestamp.s > 59) {
        throw std::invalid_argument("wrong second value in timestamp");
    }
    return timestamp;
}


TimeStampColumn::TimeStampColumn(const std::vector<std::string>& values) {
    data.reserve(values.size());
    for (const auto& s : values) {
        data.push_back(StringToTimeStamp(s));
    }
}

void TimeStampColumn::AppendStr(std::string&& s) {
    data.push_back(StringToTimeStamp(s));
}

void TimeStampColumn::PrintMf(Writer& writer) const {
    std::vector<int64_t> ints;
    ints.reserve(data.size());
    for (const auto& ts : data) {
        ints.push_back(TimeStampToInt64(ts));
    }
    bit_packing::Encode(writer, ints);
}

void TimeStampColumn::ReadMf(Reader& reader) {
    std::vector<int64_t> ints;
    bit_packing::Decode(reader, ints);
    data.resize(ints.size());
    for (size_t i = 0; i < ints.size(); ++i) {
        data[i] = Int64ToTimeStamp(ints[i]);
    }
}

void TimeStampColumn::PrintElemCsv(Writer& w, size_t i, bool b) const {
    if (i >= data.size()) {
        return;
    }
    w.WriteElem(TimeStampToString(data[i]), b);
}

std::string TimeStampColumn::GetElemToString(size_t index) const {
    if (index >= data.size()) {
        throw std::out_of_range("index out of range in GetElemToString");
    }
    return TimeStampToString(data[index]);
}

void TimeStampColumn::Accept(ColumnVisitor& visitor, const std::vector<size_t>& group_indices) const {
    visitor.Visit(*this, group_indices);
}

bool TimeStampColumn::Compare(const std::string& elem, size_t i, CompareSign sign) const {
    if (sign == CompareSign::IN) {
        throw std::invalid_argument("IN sign is not supported for TimeStampColumn");
    }
    if (sign == CompareSign::LIKE || sign == CompareSign::NOT_LIKE) {
        throw std::invalid_argument("LIKE and NOT_LIKE are supported only for string columns");
    }
    TimeStamp ts2 = StringToTimeStamp(elem);
    return column_detail::BasicCompare(TimeStampToInt64(data[i]), TimeStampToInt64(ts2), sign);
}

void TimeStampColumn::Filter(const std::string& value, CompareSign sign, std::vector<bool>& banned) const {
    if (sign == CompareSign::IN) {
        throw std::invalid_argument("IN sign is not supported for TimeStampColumn");
    }
    if (sign == CompareSign::LIKE || sign == CompareSign::NOT_LIKE) {
        throw std::invalid_argument("LIKE and NOT_LIKE are supported only for string columns");
    }
    const int64_t parsed = TimeStampToInt64(StringToTimeStamp(value));
    for (size_t i = 0; i < data.size(); ++i) {
        if (banned[i]) continue;
        if (!column_detail::BasicCompare(TimeStampToInt64(data[i]), parsed, sign)) {
            banned[i] = true;
        }
    }
}

std::shared_ptr<Column> TimeStampColumn::CopyFiltered(const std::vector<bool>& banned) const {
    return std::make_shared<TimeStampColumn>(column_detail::CopyAllowedValues(data, banned));
}

std::shared_ptr<Column> TimeStampColumn::CopyReordered(const std::vector<size_t>& ordered) const {
    return std::make_shared<TimeStampColumn>(column_detail::CopyReorderedValues(data, ordered));
}

size_t TimeStampColumn::Size() const {
    return data.size();
}

void TimeStampColumn::BinaryWriteInBuf(std::vector<char>& out, size_t index) const {
    if (index >= data.size()) {
        throw std::out_of_range("index out of range in BinaryWriteInBuf");
    }
    const int64_t v = TimeStampToInt64(data[index]);
    const char* ptr = reinterpret_cast<const char*>(&v);
    out.insert(out.end(), ptr, ptr + sizeof(v));
}

void TimeStampColumn::BinaryReadFromBuf(const char*& ptr) {
    int64_t v;
    std::memcpy(&v, ptr, sizeof(v));
    ptr += sizeof(v);
    data.push_back(Int64ToTimeStamp(v));
}

void TimeStampColumn::AppendRaw(TimeStamp timestamp) {
    data.push_back(timestamp);
}

const std::vector<TimeStamp>& TimeStampColumn::Data() const {
    return data;
}

TimeStamp TimeStampColumn::ValueAt(size_t index) const {
    if (index >= data.size()) {
        throw std::out_of_range("index out of range in ValueAt");
    }
    return data[index];
}


int TimeStampColumn::CompareAt(size_t index, const Column& other, size_t other_index) const {
    const auto& o = static_cast<const TimeStampColumn&>(other);
    int64_t t1 = TimeStampToInt64(data[index]);
    int64_t t2 = TimeStampToInt64(o.data[other_index]);
    return (t1 > t2) - (t1 < t2);
}

void TimeStampColumn::AppendFrom(const Column& other, size_t index) {
    const auto& o = static_cast<const TimeStampColumn&>(other);
    AppendRaw(o.ValueAt(index));
}