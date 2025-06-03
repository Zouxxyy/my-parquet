#include "reader/Reader.h"
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include "common/Utils.h"

namespace zouxxyy::parquet::reader {

template <typename T>
T readField(const char*& ptr) {
  T data = *reinterpret_cast<const T*>(ptr);
  ptr += sizeof(T);
  return data;
}

std::shared_ptr<thrift::FileMetaData> parseFileMetadata(
    const char* fileMetadataPtr,
    size_t len) {
  std::shared_ptr<apache::thrift::transport::TMemoryBuffer> memoryBuffer(
      new apache::thrift::transport::TMemoryBuffer());
  memoryBuffer->write(reinterpret_cast<const uint8_t*>(fileMetadataPtr), len);

  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol(
      new apache::thrift::protocol::TCompactProtocol(memoryBuffer));

  auto fileMetadata = std::make_shared<thrift::FileMetaData>();

  try {
    fileMetadata->read(protocol.get());
    return fileMetadata;
  } catch (const apache::thrift::TException& tx) {
    std::cerr << "ERROR: " << tx.what() << std::endl;
    return nullptr;
  }
}

std::pair<std::shared_ptr<thrift::PageHeader>, uint32_t> parsePageHeader(
    const char* pagePtr,
    size_t len) {
  std::shared_ptr<apache::thrift::transport::TMemoryBuffer> memoryBuffer(
      new apache::thrift::transport::TMemoryBuffer());
  memoryBuffer->write(reinterpret_cast<const uint8_t*>(pagePtr), len);

  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol(
      new apache::thrift::protocol::TCompactProtocol(memoryBuffer));

  auto pageHeader = std::make_shared<thrift::PageHeader>();

  try {
    uint32_t readBytes = pageHeader->read(protocol.get());
    return {pageHeader, readBytes};
  } catch (const apache::thrift::TException& tx) {
    std::cerr << "ERROR: " << tx.what() << std::endl;
    return {nullptr, 0};
  }
}

void readPage(
    const char* pagePtr,
    thrift::ColumnMetaData& ckMetaData,
    thrift::Type::type columnType) {
  int64_t columnChunkCompressedSize = ckMetaData.total_compressed_size;
  auto [pageHeader, pageHeaderSize] =
      parsePageHeader(pagePtr, columnChunkCompressedSize);

  // todo: using statistics to filter
  auto statistics = pageHeader->data_page_header.statistics;

  // read pageData
  // todo: add deCompression, judge snappy, zstd...
  auto compression = ckMetaData.codec;

  auto pageDataPtr = pagePtr + pageHeaderSize;
  // todo: judge defineLength
  auto defineLength = readField<int32_t>(pageDataPtr);
  pageDataPtr += defineLength;

  auto rowCount = pageHeader->data_page_header.num_values;

  // todo: add encoding, judge plan, rle...
  auto encoding = pageHeader->data_page_header.encoding;

  for (int i = 0; i < rowCount; i++) {
    // todo: support more types
    switch (columnType) {
      case thrift::Type::INT64:
        std::cout << "index: " << i
                  << ", value: " << readField<int64_t>(pageDataPtr)
                  << std::endl;
        break;
      case thrift::Type::DOUBLE:
        std::cout << "index: " << i
                  << ", value: " << readField<double>(pageDataPtr) << std::endl;
        break;
      default:
        throw std::runtime_error("unsupported type");
    }
  }
}

void readRowGroup(
    const thrift::RowGroup& rowGroup,
    const char* fileData_,
    std::vector<std::pair<std::string, thrift::Type::type>> schema,
    const std::optional<std::unordered_set<std::string>>& project) {
  for (int i = 0; i < rowGroup.columns.size(); i++) {
    // column pruning
    if (project.has_value() && !project->contains(schema[i].first)) {
      continue;
    }

    auto columnChunk = rowGroup.columns[i];
    auto ckMetaData = columnChunk.meta_data;

    // todo: using statistics to filter
    auto statistics = ckMetaData.statistics;

    int64_t dataPageOffset = ckMetaData.data_page_offset;
    // todo: how to read more pages
    auto pagePtr = fileData_ + dataPageOffset;
    auto columnType = schema[i].second;
    std::cout << "--------- column chunk: " << i << " start ------------"
              << std::endl;
    std::cout << "num values: " << ckMetaData.num_values
              << ", col name: " << schema[i].first
              << ", col type: " << schema[i].second << std::endl;
    readPage(pagePtr, ckMetaData, columnType);
    std::cout << "--------- column chunk: " << i << " end --------------"
              << std::endl;
  }
}

std::vector<std::pair<std::string, thrift::Type::type>> getSchema(
    const std::shared_ptr<thrift::FileMetaData>& fileMetaData) {
  auto schema = fileMetaData->schema;
  std::vector<std::pair<std::string, thrift::Type::type>> sc;
  for (int i = 1; i < schema.size(); i++) {
    sc.emplace_back(schema[i].name, schema[i].type);
  }
  return sc;
}

void readParquet(
    std::string& fileName,
    const std::optional<std::unordered_set<std::string>>& project) {
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
  const char* fileData_ = reinterpret_cast<const char*&>(buffer);

  // read magic number
  std::cout << "magic number is: ";
  common::printVector(fileData_, size - 4, 4);

  // read footer length
  auto footerLenPtr = fileData_ + size - 8;
  uint32_t footerLength = readField<int32_t>(footerLenPtr);
  std::cout << "footer length is: " << footerLength << std::endl;

  // parse file metadata
  auto fileMetadataPtr = fileData_ + size - 8 - footerLength;
  auto fileMetaData = parseFileMetadata(fileMetadataPtr, footerLength);

  // load schema
  auto schema = getSchema(fileMetaData);
  for (auto& [name, type] : schema) {
    std::cout << "schema col name: " << name << ", type: " << type << std::endl;
  }

  // read row groups
  auto rowGroups = fileMetaData->row_groups;
  std::cout << "row groups count: " << rowGroups.size() << std::endl;
  for (int i = 0; i < rowGroups.size(); i++) {
    std::cout << "------------ row group: " << i << " start ------------"
              << std::endl;
    readRowGroup(rowGroups[i], fileData_, schema, project);
    std::cout << "------------ row group: " << i << " end --------------\n"
              << std::endl;
  }

  file.close();
  std::cout << "end read parquet" << std::endl;
}

} // namespace zouxxyy::parquet::reader
