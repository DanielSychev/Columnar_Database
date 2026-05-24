#include "engine/data_storage/column.h"
#include "utils.h"
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

void StrColumn::AddElem(std::string&& s) {
    buf.insert(buf.end(), s.begin(), s.end());
    offsets.push_back(buf.size());
    ++count;
}

void StrColumn::PrintMf(Writer& w) const {
    w.BinaryWriteVector(offsets);
    w.BinaryWriteVector(buf);
}

void StrColumn::ReadMf(Reader& r) {
    r.BinaryReadVector(offsets);
    r.BinaryReadVector(buf);
    count = offsets.size() - 1;
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

void StrColumn::Accept(ColumnVisitor& visitor, size_t ind) const {
    visitor.Visit(*this, ind);
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

std::string StrColumn::ValueAt(size_t index) const {
    if (index >= count) {
        throw std::out_of_range("index out of range in ValueAt");
    }
    return std::string(GetElemView(index));
}


void DateColumn::AddElem(std::string&& s) {
    data.push_back(StringToDate(s));
}

DateColumn::DateColumn(const std::vector<std::string>& values) {
    data.reserve(values.size());
    for (const auto& s: values) {
        data.push_back(StringToDate(s));
    }
}

void DateColumn::PrintMf(Writer& writer) const {
    writer.BinaryWriteVector(data);
}

void DateColumn::ReadMf(Reader& reader) {
    reader.BinaryReadVector(data);
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

void DateColumn::Accept(ColumnVisitor& visitor, size_t ind) const {
    visitor.Visit(*this, ind);
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

std::shared_ptr<Column> DateColumn::CopyFiltered(const std::vector<bool>& banned) const {
    return std::make_shared<DateColumn>(column_detail::CopyAllowedValues(data, banned));
}

std::shared_ptr<Column> DateColumn::CopyReordered(const std::vector<size_t>& ordered) const {
    return std::make_shared<DateColumn>(column_detail::CopyReorderedValues(data, ordered));
}

size_t DateColumn::Size() const {
    return data.size();
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

void TimeStampColumn::AddElem(std::string&& s) {
    data.push_back(StringToTimeStamp(s));
}

void TimeStampColumn::PrintMf(Writer& writer) const {
    writer.BinaryWriteVector(data);
}

void TimeStampColumn::ReadMf(Reader& reader) {
    reader.BinaryReadVector(data);
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

void TimeStampColumn::Accept(ColumnVisitor& visitor, size_t ind) const {
    visitor.Visit(*this, ind);
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

std::shared_ptr<Column> TimeStampColumn::CopyFiltered(const std::vector<bool>& banned) const {
    return std::make_shared<TimeStampColumn>(column_detail::CopyAllowedValues(data, banned));
}

std::shared_ptr<Column> TimeStampColumn::CopyReordered(const std::vector<size_t>& ordered) const {
    return std::make_shared<TimeStampColumn>(column_detail::CopyReorderedValues(data, ordered));
}

size_t TimeStampColumn::Size() const {
    return data.size();
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

