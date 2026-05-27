add_library(executor STATIC executor.cpp operator.cpp aggregation.cpp transform.cpp)
target_link_libraries(executor PUBLIC engine re2::re2 absl::flat_hash_map)
target_include_directories(executor PUBLIC ${CMAKE_SOURCE_DIR}/src)
