add_executable(engine a.cpp)
target_include_directories(engine PUBLIC ${CMAKE_SOURCE_DIR}/src) # чтобы подтягивать .h файлики из пути папки src