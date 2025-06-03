#pragma once

#include <unordered_set>
#include "thrift/ParquetTypes.h"

namespace zouxxyy::parquet::reader {

void readParquet(
    std::string& fileName,
    const std::optional<std::unordered_set<std::string>>& project =
        std::nullopt);
} // namespace zouxxyy::parquet::reader
