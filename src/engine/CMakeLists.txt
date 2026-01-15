add_executable(engine main.cpp data_storage.cpp)
target_link_libraries(engine reader writer)
target_include_directories(engine PUBLIC ${CMAKE_SOURCE_DIR}/src) # чтобы подтягивать .h файлики из пути папки src
