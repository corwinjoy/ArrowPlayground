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
    std::default_random_engine rng(dev());
    std::uniform_real_distribution<> rand_gen(0.0, 1.0);

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> fields;

    // For simplicity, we'll create int32 columns. You can expand this to handle other types.
    for (int i = 0; i < nColumns; i++)
    {
      arrow::FloatBuilder builder;
      for (auto j = 0; j < nRows; j++)
      {
        if (!builder.Append(rand_gen(rng)).ok())
          throw std::runtime_error("builder.Append");
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

    auto begin = std::chrono::steady_clock::now();
    auto readerProperties = parquet::default_reader_properties();
    parquet::arrow::FileReaderBuilder fileReaderBuilder;
    ARROW_RETURN_NOT_OK(fileReaderBuilder.OpenFile(filename, false, readerProperties, metadata));
    auto reader = fileReaderBuilder.Build();
    // reader->init();
    auto end = std::chrono::steady_clock::now();


    *tm_builder = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);

    begin = std::chrono::steady_clock::now();
    std::shared_ptr<arrow::Table> parquet_table;
    // Read the table.
    ARROW_RETURN_NOT_OK(reader->get()->ReadTable(indicies, &parquet_table));

    end = std::chrono::steady_clock::now();
    *tm_reader = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    *tm_tot = *tm_builder + *tm_reader;
    return Status::OK();
  }

  Status ReadPageIndexes(const std::string &filename) {
    /*
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));
    auto metadata = parquet::ReadMetaData(infile);
     */

    auto read_properties = parquet::default_reader_properties();
    auto reader = parquet::ParquetFileReader::OpenFile(filename, false, read_properties);

    auto metadata = reader->metadata();

    // ASSERT_EQ(expect_num_row_groups, metadata->num_row_groups());

    std::shared_ptr<parquet::PageIndexReader> page_index_reader = reader->GetPageIndexReader();
    assert(page_index_reader != nullptr);

    page_index_reader->WillNeed({0}, {},
                                {true, true});

    // PageIndexReaderParam{{0}, {}, {true, true}}

    for (int rg = 0; rg < metadata->num_row_groups(); ++rg) {
      std::shared_ptr<parquet::RowGroupPageIndexReader> row_group_index_reader = page_index_reader->RowGroup(rg);
      assert(row_group_index_reader !=nullptr);

      auto row_group_reader = reader->RowGroup(rg);
      assert(row_group_reader != nullptr);

      for (int col = 0; col < metadata->num_columns(); ++col) {
        auto column_index = row_group_index_reader->GetColumnIndex(col);
        auto offset_index = row_group_index_reader->GetOffsetIndex(col);
      }
    }
  }

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

    std::vector<int> nColumns = {10000};
    std::vector<int64_t> chunk_sizes = {1000};
    std::vector<int> rows_list = {10000};

    std::vector<int> indicies(100);
    std::iota(indicies.begin(), indicies.end(), 0);

    for (auto chunk_size : chunk_sizes)
    {
      for (int nRow : rows_list)
      {
        for (int nColumn : nColumns)
        {
          std::chrono::microseconds writing_dt;

          if (!std::filesystem::exists(FILE_NAME))
            ARROW_RETURN_NOT_OK(WriteTableToParquet(nColumn, nRow, FILE_NAME, &writing_dt, chunk_size));

          ARROW_RETURN_NOT_OK(ReadPageIndexes(FILE_NAME));
          const int repeats = 3;
          std::vector<std::chrono::microseconds> tm_tots(repeats);
          std::vector<std::chrono::microseconds> tm_builders(repeats);
          std::vector<std::chrono::microseconds> tm_readers(repeats);
          for (int i = 0; i < repeats; i++)
          {
            // ARROW_RETURN_NOT_OK(ReadEntireTable(FILE_NAME, &reading_all_dts[i]));
            ARROW_RETURN_NOT_OK(ReadColumnsAsTableExternalMetadata(FILE_NAME, indicies, &tm_tots[i],
                                                                   &tm_builders[i], &tm_readers[i]));
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