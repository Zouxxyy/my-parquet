include_directories(.)

add_subdirectory(common)
add_subdirectory(reader)
add_subdirectory(thrift)

add_executable(my_parquet main.cpp)
target_link_libraries(my_parquet zouxxyy_parquet_reader)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/examples
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
