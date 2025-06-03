#pragma once

#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace zouxxyy::parquet::common {

void printVector(std::vector<char>& v, int start, int len);

int readInt(const std::vector<char>& buffer, size_t startPos);

} // namespace zouxxyy::parquet::common
