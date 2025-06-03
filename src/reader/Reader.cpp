#include "reader/Reader.h"
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include "common/Utils.h"

namespace zouxxyy::parquet::reader {

std::unique_ptr<thrift::FileMetaData>
parseFileMetadata(const std::vector<char>& bytes, size_t start, size_t len) {
  std::shared_ptr<apache::thrift::transport::TMemoryBuffer> memoryBuffer(
      new apache::thrift::transport::TMemoryBuffer());
  memoryBuffer->write(
      reinterpret_cast<const uint8_t*>(bytes.data()) + start, len);

  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol(
      new apache::thrift::protocol::TCompactProtocol(memoryBuffer));

  auto fileMetadata = std::make_unique<thrift::FileMetaData>();

  try {
    fileMetadata->read(protocol.get());
    return fileMetadata;
  } catch (const apache::thrift::TException& tx) {
    std::cerr << "ERROR: " << tx.what() << std::endl;
    return nullptr;
  }
}

void readParquet(std::string& fileName) {
  std::cout << "start read parquet: " << fileName << std::endl;

  // open file
  std::ifstream file(fileName, std::ios::binary | std::ios::ate);
  if (!file) {
    std::string errorMsg = std::strerror(errno);
    std::string fullErrorMsg = "unable to open '" + fileName + "': " + errorMsg;
    throw std::runtime_error(fullErrorMsg);
  }

  // read bytes to buffer
  std::streamsize size = file.tellg();
  std::cout << "file size is: " << size << std::endl;
  file.seekg(0, std::ios::beg);
  std::vector<char> buffer(size);
  file.read(buffer.data(), size);

  // read magic number
  std::cout << "magic number is: ";
  common::printVector(buffer, size - 4, 4);

  // read footer length
  int footerLength = common::readInt(buffer, size - 8);
  std::cout << "footer length is: " << footerLength << std::endl;

  // parse file metadata
  int metadataOffset = size - 8 - footerLength;
  auto fileMetaData = parseFileMetadata(buffer, metadataOffset, footerLength);

  file.close();
  std::cout << "end read parquet" << std::endl;
}

} // namespace zouxxyy::parquet::reader
