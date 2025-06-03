#pragma once

#include "thrift/ParquetTypes.h"

namespace zouxxyy::parquet::reader {

std::unique_ptr<thrift::FileMetaData>
parseFileMetadata(const std::vector<char>& bytes, size_t start, size_t len);

void readParquet(std::string& fileName);
} // namespace zouxxyy::parquet::reader
