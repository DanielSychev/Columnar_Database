add_library(reader STATIC reader.cpp)
target_include_directories(reader PUBLIC ${CMAKE_SOURCE_DIR}/src) # чтобы подтягивать .h файлики из пути папки src