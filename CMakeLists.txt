cmake_minimum_required(VERSION 3.15)
project(titankv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Source files for titankv library
set(TITANKV_SOURCES
    internal/engine2_0/bloomfilter.cpp
    internal/engine2_0/internal_key.cpp
    internal/engine2_0/wal.cpp
    internal/engine2_0/memtable.cpp
    internal/engine2_0/sstable_builder.cpp
    internal/engine2_0/sstable_reader.cpp
    internal/engine2_0/sstable_iterator.cpp
    internal/engine2_0/manifest.cpp
    internal/engine2_0/db.cpp
)

add_library(titankv_lib STATIC ${TITANKV_SOURCES})
target_include_directories(titankv_lib PUBLIC internal/engine2_0)

find_package(Threads REQUIRED)
target_link_libraries(titankv_lib PRIVATE Threads::Threads)

# Enable testing
enable_testing()

find_package(GTest REQUIRED)

add_executable(titankv_tests internal/engine2_0/test_db.cpp)
target_link_libraries(titankv_tests PRIVATE titankv_lib GTest::gtest GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(titankv_tests)
