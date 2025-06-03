add_library(zouxxyy_parquet_reader Reader.cpp)
target_link_libraries(
        zouxxyy_parquet_reader
        zouxxyy_parquet_common
        zouxxyy_parquet_thrift
)
