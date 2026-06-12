add_executable(test_reader TestReader.cpp)
target_link_libraries(test_reader reader GTest::gtest_main)
target_include_directories(test_reader PUBLIC ${CMAKE_SOURCE_DIR}/src) # чтобы подтягивать .h файлики из пути папки src
add_test(NAME test_reader COMMAND test_reader)

add_executable(test_writer TestWriter.cpp)
target_link_libraries(test_writer writer GTest::gtest_main)
target_include_directories(test_writer PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_writer COMMAND test_writer)

add_executable(test_batch_serialization TestBatchSerialization.cpp)
target_link_libraries(test_batch_serialization engine GTest::gtest_main)
target_include_directories(test_batch_serialization PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_batch_serialization COMMAND test_batch_serialization)

add_executable(test_scan_executor TestScanExecutor.cpp)
target_link_libraries(test_scan_executor executor GTest::gtest_main)
target_include_directories(test_scan_executor PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_scan_executor COMMAND test_scan_executor)

add_executable(test_utils TestUtils.cpp)
target_link_libraries(test_utils engine GTest::gtest_main)
target_include_directories(test_utils PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_utils COMMAND test_utils)

add_executable(test_schema TestSchema.cpp)
target_link_libraries(test_schema engine GTest::gtest_main)
target_include_directories(test_schema PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_schema COMMAND test_schema)

add_executable(test_column TestColumn.cpp)
target_link_libraries(test_column engine GTest::gtest_main)
target_include_directories(test_column PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_column COMMAND test_column)

add_executable(test_bit_packing TestBitPacking.cpp)
target_link_libraries(test_bit_packing reader writer GTest::gtest_main)
target_include_directories(test_bit_packing PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_bit_packing COMMAND test_bit_packing)

add_executable(test_dictionary_encoding TestDictionaryEncoding.cpp)
target_link_libraries(test_dictionary_encoding reader writer GTest::gtest_main)
target_include_directories(test_dictionary_encoding PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_dictionary_encoding COMMAND test_dictionary_encoding)

add_executable(test_aggregation TestAggregation.cpp)
target_link_libraries(test_aggregation executor GTest::gtest_main)
target_include_directories(test_aggregation PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_aggregation COMMAND test_aggregation)

add_executable(test_transform TestTransform.cpp)
target_link_libraries(test_transform executor GTest::gtest_main)
target_include_directories(test_transform PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_transform COMMAND test_transform)

add_executable(test_groupby_pipeline TestGroupByPipeline.cpp)
target_link_libraries(test_groupby_pipeline executor GTest::gtest_main)
target_include_directories(test_groupby_pipeline PUBLIC ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_groupby_pipeline COMMAND test_groupby_pipeline)
