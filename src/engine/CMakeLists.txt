add_library(engine STATIC
    data_storage/schema.cpp
    data_storage/batch.cpp
    data_storage/column.cpp
    serialization/batch_serialization.cpp
    engine.cpp
)
target_link_libraries(engine PUBLIC reader writer)
target_include_directories(engine PUBLIC ${CMAKE_SOURCE_DIR}/src) # чтобы подтягивать .h файлики из пути папки src

add_executable(engine_cli main.cpp)
target_link_libraries(engine_cli PRIVATE engine)
