#include "Utils.h"
#include <cstring>
#include <iostream>
#include <vector>

namespace zouxxyy::parquet::common {

void printVector(const char*& v, int start, int len) {
  int end = start + len;
  for (int i = start; i < end; i++) {
    std::cout << v[i];
  }
  std::cout << std::endl;
}

int readInt(const std::vector<char>& buffer, size_t startPos) {
  if (startPos + sizeof(int) > buffer.size()) {
    throw std::out_of_range("Not enough bytes in buffer to read an int");
  }

  int result;
  std::memcpy(&result, &buffer[startPos], sizeof(int));
  return result;
}
} // namespace zouxxyy::parquet::common
