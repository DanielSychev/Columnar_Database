#include <CSV_writer/writer.h>
#include <engine/data_storage.h>
#include <cinttypes>
#include <memory>

// Writer::Writer(const std::& file_path, char delimetr) : delimetr_(delimetr) {
//     out_ = new std::ofstream(file_path);
// }
Writer::Writer(std::ostream& ss, char delimetr) : out_(ss), delimetr_(delimetr) {
}

void Writer::WriteDataStorage(const DataStorage& data) {
    data.Print(*this);
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
    s.find('"') == std::string::npos) {
        out_ << s;
        CheckFlag(fl);
    }
}