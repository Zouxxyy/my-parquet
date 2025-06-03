add_library(zouxxyy_parquet_thrift ParquetTypes.cpp)
target_link_libraries(
        zouxxyy_parquet_thrift
        thrift
)