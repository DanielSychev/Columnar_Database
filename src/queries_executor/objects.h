// #include <utils.h>
#include "engine/data_storage/batch.h"
#include <memory>

// template<concepts::BinarySerializable T>
// class ConstantBatch : public Batch {
// public:
//     ConstantBatch(T value) {

//     }
// private:
//     T value;
// };

template<concepts::BinarySerializable T>
std::shared_ptr<Batch> CreateConstantBatch(T value) {
    auto batch = std::make_shared<Batch>(Schema({"constant"}, {Type::int64}), 1);
    std::vector<std::string> row = {std::to_string(value)};
    batch->AddRow(std::move(row));
    return batch;
}