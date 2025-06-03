#include "reader/Reader.h"

using namespace zouxxyy::parquet;

int main() {
  std::string fileName = "examples/sample.parquet";
  reader::readParquet(fileName);
  return 0;
}
