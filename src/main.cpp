#include <unordered_set>
#include "reader/Reader.h"

using namespace zouxxyy::parquet;

int main() {
  std::string fileName = "examples/sample.parquet";
  std::unordered_set<std::string> project = {"a", "b"};
  std::optional<std::unordered_set<std::string>> projectOpt(project);
  reader::readParquet(fileName, projectOpt);
  return 0;
}
