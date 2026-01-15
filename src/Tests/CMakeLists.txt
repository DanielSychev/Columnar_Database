add_executable(test_read TestRead.cpp)
target_link_libraries(test_read reader GTest::gtest_main)
target_include_directories(test_read PUBLIC ${CMAKE_SOURCE_DIR}/src) # чтобы подтягивать .h файлики из пути папки src