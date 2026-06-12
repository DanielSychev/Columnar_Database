if(APPLE)
    list(APPEND CMAKE_PREFIX_PATH /opt/homebrew)
endif()

find_package(Boost 1.81 REQUIRED)

find_package(re2 QUIET)

if(NOT re2_FOUND)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(RE2 REQUIRED re2)
    add_library(re2::re2 INTERFACE IMPORTED)
    target_include_directories(re2::re2 INTERFACE ${RE2_INCLUDE_DIRS})
    target_link_libraries(re2::re2 INTERFACE ${RE2_LINK_LIBRARIES})
endif()

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
endif()
