add_subdirectory(CsvMfReader)
add_subdirectory(CsvMfWriter)
add_subdirectory(engine)
add_subdirectory(queries_executor)

if(BUILD_TESTING)
    add_subdirectory(Tests)
endif()
