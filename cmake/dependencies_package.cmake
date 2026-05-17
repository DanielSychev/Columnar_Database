if(APPLE)
    list(APPEND CMAKE_PREFIX_PATH /opt/homebrew)
endif()

find_package(re2 REQUIRED)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
endif()
