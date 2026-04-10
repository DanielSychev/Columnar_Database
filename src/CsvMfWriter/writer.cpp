#include <CsvMfWriter/writer.h>

namespace {
bool NeedsEscaping(std::string_view s, char delimiter) {
    return s.find(delimiter) != std::string::npos ||
        s.find('"') != std::string::npos ||
        s.find('\n') != std::string::npos ||
        s.find('\r') != std::string::npos;
}

std::string EscapeField(std::string_view s) {
    size_t quote_count = 0;
    for (char c : s) {
        if (c == '"') {
            ++quote_count;
        }
    }

    std::string escaped;
    escaped.reserve(s.size() + quote_count + 2);
    escaped.push_back('"');
    for (char c : s) {
        if (c == '"') {
            escaped.push_back('"');
            escaped.push_back('"');
        } else {
            escaped.push_back(c);
        }
    }
    escaped.push_back('"');
    return escaped;
}
}

Writer::Writer(std::ostream& ss, char delimetr) : out_(ss), delimetr_(delimetr) {
}

void Writer::CheckFlag(bool fl) {
    out_.put(fl ? '\n' : delimetr_);
}

void Writer::WriteElem(int64_t x, bool fl) {
    out_ << x;
    CheckFlag(fl);
}

void Writer::WriteElem(std::string_view s, bool fl) {
    if (!NeedsEscaping(s, delimetr_)) {
        out_.write(s.data(), static_cast<std::streamsize>(s.size()));
        CheckFlag(fl);
        return;
    }

    const std::string escaped = EscapeField(s);
    out_.write(escaped.data(), static_cast<std::streamsize>(escaped.size()));
    CheckFlag(fl);
}

size_t Writer::TellPos() {
    return out_.tellp();
}
