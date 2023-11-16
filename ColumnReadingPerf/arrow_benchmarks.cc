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

using arrow::Status;

namespace
{
  // const char *FILE_NAME = "/mnt/ramfs/my.parquet";
  const char *FILE_NAME = "/tmp/my.parquet";

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
                          .max_row_group_length(chunkSize)
                          ->disable_dictionary()
                          ->disable_statistics()
                          ->build();
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, chunkSize, properties));
    auto end = std::chrono::steady_clock::now();
    *dt = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    return Status::OK();
  }

  Status ReadColumnsAsTableExternalMetadata(const std::string &filename, std::vector<int> indicies, std::chrono::microseconds *dt, std::chrono::microseconds *dt1, std::chrono::microseconds *dt2)
  {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));
    auto metadata = parquet::ReadMetaData(infile);

    auto begin = std::chrono::steady_clock::now();
    auto readerProperties = parquet::default_reader_properties();
    parquet::arrow::FileReaderBuilder fileReaderBuilder;
    ARROW_RETURN_NOT_OK(fileReaderBuilder.OpenFile(filename, false, readerProperties, metadata));
    auto reader = fileReaderBuilder.Build();
    // reader->init();
    auto end = std::chrono::steady_clock::now();

    *dt1 = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);

    begin = std::chrono::steady_clock::now();
    std::shared_ptr<arrow::Table> parquet_table;
    // Read the table.
    ARROW_RETURN_NOT_OK(reader->get()->ReadTable(indicies, &parquet_table));

    end = std::chrono::steady_clock::now();
    *dt2 = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    *dt = *dt1 + *dt2;
    return Status::OK();
  }

  Status RunMain(int argc, char **argv)
  {
    std::ofstream csvFile;
    csvFile.open("arrow_results.csv", std::ios_base::out); // append instead of overwrite
    csvFile << "name,columns,rows,row_groups,data_page_size,columns_to_read,reading(μs),reading_p1(μs),reading_p2(μs)" << std::endl;

    std::vector<int> nColumns = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
    std::vector<int64_t> chunk_sizes = {10000, 100000};
    std::vector<int> rows_list = {10000, 50000};

    std::vector<int> indicies(100);
    std::iota(indicies.begin(), indicies.end(), 0);

    for (auto chunk_size : chunk_sizes)
    {
      for (int nRow : rows_list)
      {
        for (int nColumn : nColumns)
        {
          std::chrono::microseconds writing_dt;

          // if (!std::filesystem::exists(FILE_NAME))
          ARROW_RETURN_NOT_OK(WriteTableToParquet(nColumn, nRow, FILE_NAME, &writing_dt, chunk_size));

          const int repeats = 3;
          //const int repeats = 3000;
          std::vector<std::chrono::microseconds> reading_100_dts(repeats);
          std::vector<std::chrono::microseconds> reading_100_dts1(repeats);
          std::vector<std::chrono::microseconds> reading_100_dts2(repeats);
          for (int i = 0; i < repeats; i++)
          {
            // ARROW_RETURN_NOT_OK(ReadEntireTable(FILE_NAME, &reading_all_dts[i]));
            ARROW_RETURN_NOT_OK(ReadColumnsAsTableExternalMetadata(FILE_NAME, indicies, &reading_100_dts[i], &reading_100_dts1[i], &reading_100_dts2[i]));
          }

          auto reading_100_dt = *std::min_element(reading_100_dts.begin(), reading_100_dts.end());
          auto reading_100_dt1 = *std::min_element(reading_100_dts1.begin(), reading_100_dts1.end());
          auto reading_100_dt2 = *std::min_element(reading_100_dts2.begin(), reading_100_dts2.end());

          std::cerr << "(" << nColumn << ", " << nRow << ")"
                    << ", chunk_size=" << chunk_size
                    << ", writing_dt=" << writing_dt.count() / nColumn
                    << ", reading_100_dt=" << reading_100_dt.count()
                    // << ", reading_100_dt=" << reading_100_dt.count() / 100
                    // << ", reading_100_dt1=" << reading_100_dt1.count() / 100
                    << ", reading_100_dt1=" << reading_100_dt1.count()
                    // << ", reading_100_dt2=" << reading_100_dt2.count() / 100
                    << ", reading_100_dt2=" << reading_100_dt2.count()
                    << std::endl;

          csvFile << "cpp_fast_with_external_metadata"
                  << ","
                  << nColumn << ","
                  << nRow << ","
                  << chunk_size << ","
                  << 1024 * 1024 * 1024 << ","
                  << 100 << ","
                  << reading_100_dt.count() << ","
                  << reading_100_dt1.count() << ","
                  << reading_100_dt2.count()
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