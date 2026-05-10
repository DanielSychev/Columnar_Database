add_executable(test_reader TestReader.cpp)
target_link_libraries(test_reader reader GTest::gtest_main)
target_include_directories(test_reader PUBLIC ${CMAKE_SOURCE_DIR}/src) # чтобы подтягивать .h файлики из пути папки src
add_test(NAME test_reader COMMAND test_reader)

add_executable(test_writer TestWriter.cpp)
target_link_libraries(test_writer writer GTest::gtest_main)
target_include_directories(test_writer PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_writer COMMAND test_writer)

add_executable(test_batch_serialization
    TestBatchSerialization.cpp
    ../engine/data_storage/schema.cpp
    ../engine/data_storage/batch.cpp
    ../engine/data_storage/column.cpp
    ../engine/serialization/batch_serialization.cpp
)
target_link_libraries(test_batch_serialization reader writer GTest::gtest_main)
target_include_directories(test_batch_serialization PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_batch_serialization COMMAND test_batch_serialization)

add_executable(test_scan_executor TestScanExecutor.cpp)
target_link_libraries(test_scan_executor executor GTest::gtest_main)
target_include_directories(test_scan_executor PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_scan_executor COMMAND test_scan_executor)
