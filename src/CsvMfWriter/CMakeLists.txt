add_library(writer STATIC writer.cpp)
target_include_directories(writer PUBLIC ${CMAKE_SOURCE_DIR}/src)