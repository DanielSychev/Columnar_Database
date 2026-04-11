add_library(executor STATIC executor.cpp operator.cpp)
target_link_libraries(executor PUBLIC engine)
target_include_directories(executor PUBLIC ${CMAKE_SOURCE_DIR}/src)