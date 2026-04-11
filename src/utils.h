#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace Constants {
    const size_t BATCH_SIZE = 1000;
    const size_t MAX_COLUMN_COUNT = UINT32_MAX;
}

namespace concepts {
    template<typename T>
    concept BinarySerializable = 
        std::is_integral_v<T> || 
        std::is_same_v<T, float> || 
        std::is_same_v<T, double>;
};