#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include <iostream>
#include <list>
#include <chrono>
#include <random>
#include <vector>
#include <fstream>
#include <filesystem>
#include <iomanip>
#include <cassert>
#include <parquet/page_index.h>


using arrow::Status;

namespace
{
  // const char *FILE_NAME = "/mnt/ramfs/my.parquet";
  // const char *FILE_NAME = "/tmp/my.parquet";
  const char *FILE_NAME = "/tmp/ColumnReadingPerf/cmake-build-debug-docker_dbarrow/my.parquet";

  std::shared_ptr<arrow::Table> GetTable(size_t nColumns, size_t nRows)
  {
    std::random_device dev;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> fields;

    // For simplicity, we'll create int32 columns. You can expand this to handle other types.
    for (int i = 0; i < nColumns; i++)
    {
      arrow::FloatBuilder builder;

      for (int j = 0; j < nRows; j++)
      {
        int reps = j / 10;
        if (j % 10 < reps) {
          float val = (j / 10) * 10;
          builder.Append(val);
        } else {
          builder.Append(static_cast<float>(j));
        }
      }

      std::shared_ptr<arrow::Array> array;
      if (!builder.Finish(&array).ok())
        throw std::runtime_error("builder.Finish");

      arrays.push_back(array);
      fields.push_back(arrow::field("c_" + std::to_string(i), arrow::float32(), false));
    }

    auto table = arrow::Table::Make(arrow::schema(fields), arrays);
    return table;
  }

  Status WriteTableToParquet(size_t nColumns, size_t nRows, const std::string &filename, std::chrono::microseconds *dt, int64_t chunkSize)
  {
    auto table = GetTable(nColumns, nRows);
    auto begin = std::chrono::steady_clock::now();
    auto result = arrow::io::FileOutputStream::Open(filename);
    auto outfile = result.ValueOrDie();

    parquet::WriterProperties::Builder builder;
    auto properties = builder
                          .enable_write_page_index()
                          ->max_row_group_length(chunkSize)
                          // ->disable_dictionary()
                          // ->disable_statistics()
                          ->build();
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, chunkSize, properties));
    auto end = std::chrono::steady_clock::now();
    *dt = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    return Status::OK();
  }

  Status ReadColumnsAsTableExternalMetadata(const std::string &filename, std::vector<int> indicies, std::chrono::microseconds *tm_tot,
                                            std::chrono::microseconds *tm_builder, std::chrono::microseconds *tm_reader)
  {

    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));
    auto metadata = parquet::ReadMetaData(infile);
    // PrintSchema(metadata->schema()->schema_root().get(), std::cout);

    int row_group = 1;
    std::vector<int> row_groups = {row_group};
    auto single_row_metadata = metadata.get()->Subset(row_groups);

    auto begin = std::chrono::steady_clock::now();
    auto readerProperties = parquet::default_reader_properties();
    parquet::arrow::FileReaderBuilder fileReaderBuilder;
    ARROW_RETURN_NOT_OK(fileReaderBuilder.OpenFile(filename, false, readerProperties, single_row_metadata));
    auto reader = fileReaderBuilder.Build();
    auto end = std::chrono::steady_clock::now();

    *tm_builder = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);

    begin = std::chrono::steady_clock::now();
    std::shared_ptr<arrow::Table> parquet_table;
    // Read the table.
    ARROW_RETURN_NOT_OK(reader->get()->ReadTable(indicies, &parquet_table));
    end = std::chrono::steady_clock::now();

    std::cout << parquet_table->ToString() << std::endl;

    *tm_reader = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    *tm_tot = *tm_builder + *tm_reader;
    return Status::OK();
  }

typedef std::vector<std::shared_ptr<parquet::OffsetIndex>> offsets;

std::vector<offsets> ReadPageIndexes(const std::string &filename) {
  auto read_properties = parquet::default_reader_properties();
  auto reader = parquet::ParquetFileReader::OpenFile(filename, false, read_properties);
  auto metadata = reader->metadata();
  std::shared_ptr<parquet::PageIndexReader> page_index_reader = reader->GetPageIndexReader();

  std::vector<offsets> rowgroup_offsets;
  for (int rg = 0; rg < metadata->num_row_groups(); ++rg) {
    std::shared_ptr<parquet::RowGroupPageIndexReader> row_group_index_reader = page_index_reader->RowGroup(rg);
    auto row_group_reader = reader->RowGroup(rg);
    offsets offset_indexes;
    for (int col = 0; col < metadata->num_columns(); ++col) {
      auto column_index = row_group_index_reader->GetColumnIndex(col);
      auto offset_index = row_group_index_reader->GetOffsetIndex(col);
      offset_indexes.push_back(offset_index);
    }
    rowgroup_offsets.push_back(offset_indexes);
  }
  return rowgroup_offsets;
}

Status ReadAndPrintMetadataRow(const std::string &filename, std::vector<int> indicies,
                               std::shared_ptr<parquet::FileMetaData> metadata) {
  auto readerProperties = parquet::default_reader_properties();
  parquet::arrow::FileReaderBuilder fileReaderBuilder;
  ARROW_RETURN_NOT_OK(fileReaderBuilder.OpenFile(filename, false, readerProperties, metadata));
  auto reader = fileReaderBuilder.Build();

  // Read and print the table.
  std::shared_ptr<arrow::Table> parquet_table;
  ARROW_RETURN_NOT_OK(reader->get()->ReadTable(indicies, &parquet_table));
  std::cout << std::endl;
  std::cout << "****************************************************************************************" << std::endl;
  std::cout << parquet_table->ToString() << std::endl;
  std::cout << "****************************************************************************************" << std::endl;

  return Status::OK();
}

Status ReadColumnsUsingOffsetIndex(const std::string &filename, std::vector<int> indicies)
{
  auto rowgroup_offsets = ReadPageIndexes(filename);
  std::shared_ptr<arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));
  auto metadata = parquet::ReadMetaData(infile);
  // PrintSchema(metadata->schema()->schema_root().get(), std::cout);

  int row_group = 6;
  std::vector<int> row_groups = {row_group};
  auto row_0_metadata = metadata->Subset({0});
  auto target_metadata = metadata->Subset({row_group});
  auto shifted_metadata = metadata->Subset({0}); // make a copy

  auto target_column_offsets = rowgroup_offsets[row_group];
  int64_t total_rows = metadata->num_rows();
  int64_t chunk_rows = row_0_metadata->num_rows();
  int64_t num_values = chunk_rows;
  if (row_group >= total_rows/chunk_rows) {
    // last page, set num_values to remainder
    num_values = total_rows % chunk_rows;
  }
  shifted_metadata->set_column_offsets(target_column_offsets, num_values);

  std::cout << "Correct read:";
  ARROW_RETURN_NOT_OK(ReadAndPrintMetadataRow(filename, indicies, target_metadata));
  std::cout << "\n\nOffset based read:";
  ARROW_RETURN_NOT_OK(ReadAndPrintMetadataRow(filename, indicies, shifted_metadata));


  return Status::OK();
}


// Examples from ArrowDevDbg/src/arrow/cpp/examples/parquet/parquet_arrow/reader_writer.cc

// #3: Read only a single RowGroup of the parquet file
void read_single_rowgroup() {
  std::cout << "Reading first RowGroup of parquet-arrow-example.parquet" << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile,
                          arrow::io::ReadableFile::Open("parquet-arrow-example.parquet",
                                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
          parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->RowGroup(0)->ReadTable(&table));
  std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns()
            << " columns." << std::endl;
}

// #4: Read only a single column of the whole parquet file
void read_single_column() {
  std::cout << "Reading first column of parquet-arrow-example.parquet" << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile,
                          arrow::io::ReadableFile::Open("parquet-arrow-example.parquet",
                                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
          parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::ChunkedArray> array;
  PARQUET_THROW_NOT_OK(reader->ReadColumn(0, &array));
  PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*array, 4, &std::cout));
  std::cout << std::endl;
}

// #5: Read only a single column of a RowGroup (this is known as ColumnChunk)
//     from the Parquet file.
void read_single_column_chunk() {
  std::cout << "Reading first ColumnChunk of the first RowGroup of "
               "parquet-arrow-example.parquet"
            << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile,
                          arrow::io::ReadableFile::Open("parquet-arrow-example.parquet",
                                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
          parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::ChunkedArray> array;
  PARQUET_THROW_NOT_OK(reader->RowGroup(0)->Column(0)->Read(&array));
  PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*array, 4, &std::cout));
  std::cout << std::endl;
}

/*
// Code to roundtrip an OffsetIndex

TEST(PageIndex, WriteOffsetIndex) {
  /// Create offset index via the OffsetIndexBuilder interface.
  auto builder = OffsetIndexBuilder::Make();
  const size_t num_pages = 5;
  const std::vector<int64_t> offsets = {100, 200, 300, 400, 500};
  const std::vector<int32_t> page_sizes = {1024, 2048, 3072, 4096, 8192};
  const std::vector<int64_t> first_row_indices = {0, 10000, 20000, 30000, 40000};
  for (size_t i = 0; i < num_pages; ++i) {
    builder->AddPage(offsets[i], page_sizes[i], first_row_indices[i]);
  }
  const int64_t final_position = 4096;
  builder->Finish(final_position);

  std::vector<std::unique_ptr<OffsetIndex>> offset_indexes;
  /// 1st element is the offset index just built.
  offset_indexes.emplace_back(builder->Build());
  /// 2nd element is the offset index restored by serialize-then-deserialize round trip.
  auto sink = CreateOutputStream();
  builder->WriteTo(sink.get());
  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());
  offset_indexes.emplace_back(OffsetIndex::Make(buffer->data(),
                                                static_cast<uint32_t>(buffer->size()),
                                                default_reader_properties()));

  /// Verify the data of the offset index.
  for (const auto& offset_index : offset_indexes) {
    ASSERT_EQ(num_pages, offset_index->page_locations().size());
    for (size_t i = 0; i < num_pages; ++i) {
      const auto& page_location = offset_index->page_locations().at(i);
      ASSERT_EQ(offsets[i] + final_position, page_location.offset);
      ASSERT_EQ(page_sizes[i], page_location.compressed_page_size);
      ASSERT_EQ(first_row_indices[i], page_location.first_row_index);
    }
  }
}
 */

/*

// TODO: Create fake metadata. From ArrowDevDbg/src/arrow/cpp/src/parquet/page_index_test.cc

struct PageIndexRanges {
  int64_t column_index_offset;
  int64_t column_index_length;
  int64_t offset_index_offset;
  int64_t offset_index_length;
};

using RowGroupRanges = std::vector<PageIndexRanges>;

/// Creates an FileMetaData object w/ single row group based on data in
/// 'row_group_ranges'. It sets the offsets and sizes of the column index and offset index
/// members of the row group. It doesn't set the member if the input value is -1.
std::shared_ptr<FileMetaData> ConstructFakeMetaData(
    const RowGroupRanges& row_group_ranges) {
  format::RowGroup row_group;
  for (auto& page_index_ranges : row_group_ranges) {
    format::ColumnChunk col_chunk;
    if (page_index_ranges.column_index_offset != -1) {
      col_chunk.__set_column_index_offset(page_index_ranges.column_index_offset);
    }
    if (page_index_ranges.column_index_length != -1) {
      col_chunk.__set_column_index_length(
          static_cast<int32_t>(page_index_ranges.column_index_length));
    }
    if (page_index_ranges.offset_index_offset != -1) {
      col_chunk.__set_offset_index_offset(page_index_ranges.offset_index_offset);
    }
    if (page_index_ranges.offset_index_length != -1) {
      col_chunk.__set_offset_index_length(
          static_cast<int32_t>(page_index_ranges.offset_index_length));
    }
    row_group.columns.push_back(col_chunk);
  }

  format::FileMetaData metadata;
  metadata.row_groups.push_back(row_group);

  metadata.schema.emplace_back();
  schema::NodeVector fields;
  for (size_t i = 0; i < row_group_ranges.size(); ++i) {
    fields.push_back(schema::Int64(std::to_string(i)));
    metadata.schema.emplace_back();
    fields.back()->ToParquet(&metadata.schema.back());
  }
  schema::GroupNode::Make("schema", Repetition::REPEATED, fields)
      ->ToParquet(&metadata.schema.front());

  auto sink = CreateOutputStream();
  ThriftSerializer{}.Serialize(&metadata, sink.get());
  auto buffer = sink->Finish().MoveValueUnsafe();
  uint32_t len = static_cast<uint32_t>(buffer->size());
  return FileMetaData::Make(buffer->data(), &len);
}

/// Validates that 'DeterminePageIndexRangesInRowGroup()' selects the expected file
/// offsets and sizes or returns false when the row group doesn't have a page index.
void ValidatePageIndexRange(const RowGroupRanges& row_group_ranges,
                            const std::vector<int32_t>& column_indices,
                            bool expected_has_column_index,
                            bool expected_has_offset_index, int expected_ci_start,
                            int expected_ci_size, int expected_oi_start,
                            int expected_oi_size) {
  auto file_metadata = ConstructFakeMetaData(row_group_ranges);
  auto read_range = PageIndexReader::DeterminePageIndexRangesInRowGroup(
      *file_metadata->RowGroup(0), column_indices);
  ASSERT_EQ(expected_has_column_index, read_range.column_index.has_value());
  ASSERT_EQ(expected_has_offset_index, read_range.offset_index.has_value());
  if (expected_has_column_index) {
    EXPECT_EQ(expected_ci_start, read_range.column_index->offset);
    EXPECT_EQ(expected_ci_size, read_range.column_index->length);
  }
  if (expected_has_offset_index) {
    EXPECT_EQ(expected_oi_start, read_range.offset_index->offset);
    EXPECT_EQ(expected_oi_size, read_range.offset_index->length);
  }
}

/// This test constructs a couple of artificial row groups with page index offsets in
/// them. Then it validates if PageIndexReader::DeterminePageIndexRangesInRowGroup()
/// properly computes the file range that contains the whole page index.
TEST(PageIndex, DeterminePageIndexRangesInRowGroup) {
  // No Column chunks
  ValidatePageIndexRange({}, {}, false, false, -1, -1, -1, -1);
  // No page index at all.
  ValidatePageIndexRange({{-1, -1, -1, -1}}, {}, false, false, -1, -1, -1, -1);
  // Page index for single column chunk.
  ValidatePageIndexRange({{10, 5, 15, 5}}, {}, true, true, 10, 5, 15, 5);
  // Page index for two column chunks.
  ValidatePageIndexRange({{10, 5, 30, 25}, {15, 15, 50, 20}}, {}, true, true, 10, 20, 30,
                         40);
  // Page index for second column chunk.
  ValidatePageIndexRange({{-1, -1, -1, -1}, {20, 10, 30, 25}}, {}, true, true, 20, 10, 30,
                         25);
  // Page index for first column chunk.
  ValidatePageIndexRange({{10, 5, 15, 5}, {-1, -1, -1, -1}}, {}, true, true, 10, 5, 15,
                         5);
  // Missing offset index for first column chunk. Gap in column index.
  ValidatePageIndexRange({{10, 5, -1, -1}, {20, 10, 30, 25}}, {}, true, true, 10, 20, 30,
                         25);
  // Missing offset index for second column chunk.
  ValidatePageIndexRange({{10, 5, 25, 5}, {20, 10, -1, -1}}, {}, true, true, 10, 20, 25,
                         5);
  // Four column chunks.
  ValidatePageIndexRange(
      {{100, 10, 220, 30}, {110, 25, 250, 10}, {140, 30, 260, 40}, {200, 10, 300, 100}},
      {}, true, true, 100, 110, 220, 180);
}

*/

  Status RunMain(int argc, char **argv)
  {
    std::ofstream csvFile;
    csvFile.open("arrow_results.csv", std::ios_base::out); // append instead of overwrite
    csvFile << "name,columns,rows,row_groups,data_page_size,columns_to_read,reading(μs),reading_p1(μs),reading_p2(μs)" << std::endl;

    /*
    std::vector<int> nColumns = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
    std::vector<int64_t> chunk_sizes = {10000, 100000};
    std::vector<int> rows_list = {10000, 50000};
     */

    std::vector<int> nColumns = {10};
    std::vector<int64_t> chunk_sizes = {10};
    std::vector<int> rows_list = {95};

    std::vector<int> indicies(nColumns[0]/2);
    std::iota(indicies.begin(), indicies.end(), 0);

    for (auto chunk_size : chunk_sizes)
    {
      for (int nRow : rows_list)
      {
        for (int nColumn : nColumns)
        {
          std::chrono::microseconds writing_dt;

          if (true || !std::filesystem::exists(FILE_NAME))
            ARROW_RETURN_NOT_OK(WriteTableToParquet(nColumn, nRow, FILE_NAME, &writing_dt, chunk_size));

          // ARROW_RETURN_NOT_OK(ReadPageIndexes(FILE_NAME));
          const int repeats = 1;
          std::vector<std::chrono::microseconds> tm_tots(repeats);
          std::vector<std::chrono::microseconds> tm_builders(repeats);
          std::vector<std::chrono::microseconds> tm_readers(repeats);
          for (int i = 0; i < repeats; i++)
          {
            ARROW_RETURN_NOT_OK(ReadColumnsUsingOffsetIndex(FILE_NAME, indicies));
            /*
            ARROW_RETURN_NOT_OK(ReadColumnsAsTableExternalMetadata(FILE_NAME, indicies, &tm_tots[i],
                                                                   &tm_builders[i], &tm_readers[i]));
            */
          }


          auto tm_tot = *std::min_element(tm_tots.begin(), tm_tots.end());
          auto tm_builder = *std::min_element(tm_builders.begin(), tm_builders.end());
          auto tm_reader = *std::min_element(tm_readers.begin(), tm_readers.end());

          std::cerr << "(" << nColumn << ", " << nRow << ")"
                    << ", chunk_size=" << chunk_size
                    << ", writing_dt=" << writing_dt.count() / nColumn
                    << ", time_total=" << tm_tot.count()
                    << ", time_builder=" << tm_builder.count()
                    << ", time_reader=" << tm_reader.count()
                    << std::endl;

          csvFile << "cpp_fast_with_external_metadata"
                  << ","
                  << nColumn << ","
                  << nRow << ","
                  << chunk_size << ","
                  << 1024 * 1024 * 1024 << ","
                  << 100 << ","
                  << tm_tot.count() << ","
                  << tm_builder.count() << ","
                  << tm_reader.count()
                  << std::endl;
        }
      }
    }

    return Status::OK();
  }
} // namespace

int main(int argc, char **argv)
{
  Status st = RunMain(argc, argv);
  if (!st.ok())
  {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}