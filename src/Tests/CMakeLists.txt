add_executable(test_reader TestReader.cpp)
target_link_libraries(test_reader reader GTest::gtest_main)
target_include_directories(test_reader PUBLIC ${CMAKE_SOURCE_DIR}/src) # чтобы подтягивать .h файлики из пути папки src

add_executable(test_writer TestWriter.cpp)
target_link_libraries(test_writer writer GTest::gtest_main)
target_include_directories(test_writer PUBLIC ${CMAKE_SOURCE_DIR}/src)
