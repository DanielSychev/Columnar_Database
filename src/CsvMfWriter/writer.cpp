#include <CsvMfWriter/writer.h>

Writer::Writer(std::ostream& ss, char delimetr) : out_(ss), delimetr_(delimetr) {
}

void Writer::CheckFlag(bool fl) {
    if (!fl) {
        out_ << ',';
    } else {
        out_ << '\n';
    }
}

void Writer::WriteElem(int64_t x, bool fl) {
    out_ << x;
    CheckFlag(fl);
}

void Writer::WriteElem(const std::string& s, bool fl) {
    if (s.find(delimetr_) == std::string::npos &&
    s.find('"') == std::string::npos &&
    s.find('\n') == std::string::npos &&
    s.find('\r') == std::string::npos) {
        out_ << s;
        CheckFlag(fl);
        return;
    }
    out_ << '"';
    for (auto& c : s) {
        if (c == '"') {
            out_ << R"("")";
        } else {
            out_ << c;
        }
    }
    out_ << '"';
    CheckFlag(fl);
}

size_t Writer::TellPos() {
    return out_.tellp();
}
