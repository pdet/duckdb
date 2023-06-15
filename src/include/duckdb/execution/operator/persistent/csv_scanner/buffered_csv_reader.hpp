//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_scanner/buffered_csv_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/csv_scanner/base_csv_reader.hpp"

namespace duckdb {
struct CopyInfo;
struct CSVFileHandle;
struct FileHandle;
struct StrpTimeFormat;

class FileOpener;
class FileSystem;

//! Buffered CSV reader is a class that reads values from a stream and parses them as a CSV file
class BufferedCSVReader : public BaseCSVReader {
	//! Initial buffer read size; can be extended for long lines
	static constexpr idx_t INITIAL_BUFFER_SIZE = 16384;
	//! Larger buffer size for non disk files
	static constexpr idx_t INITIAL_BUFFER_SIZE_LARGE = 10000000; // 10MB

public:
	BufferedCSVReader(ClientContext &context, CSVReaderOptions options,
	                  const vector<LogicalType> &requested_types = vector<LogicalType>());
	BufferedCSVReader(ClientContext &context, string filename, CSVReaderOptions options,
	                  const vector<LogicalType> &requested_types = vector<LogicalType>());
	virtual ~BufferedCSVReader() {
	}

	unsafe_unique_array<char> buffer;
	idx_t buffer_size;
	idx_t position;
	idx_t start = 0;

	vector<unsafe_unique_array<char>> cached_buffers;

	unique_ptr<CSVFileHandle> file_handle;

public:
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(DataChunk &insert_chunk);
	static string ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column, const vector<string> &names);

private:
	//! Initialize Parser
	void Initialize(const vector<LogicalType> &requested_types);
	//! Skips skip_rows, reads header row from input stream
	void SkipRowsAndReadHeader(idx_t skip_rows, bool skip_header);
	//! Jumps back to the beginning of input stream and resets necessary internal states
	void JumpToBeginning(idx_t skip_rows, bool skip_header);
	//! Resets the buffer
	void ResetBuffer();
	//! Resets the steam
	void ResetStream();
	//! Reads a new buffer from the CSV file if the current one has been exhausted
	bool ReadBuffer(idx_t &start, idx_t &line_start);
	//! Jumps back to the beginning of input stream and resets necessary internal states
	bool JumpToNextSample();
	//! Try to parse a single datachunk from the file. Throws an exception if anything goes wrong.
	void ParseCSV(ParserMode mode);
	//! Try to parse a single datachunk from the file. Returns whether or not the parsing is successful
	bool TryParseCSV(ParserMode mode);
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	bool TryParseCSV(ParserMode mode, DataChunk &insert_chunk, string &error_message);

	//! Parses a CSV file with a one-byte delimiter, escape and quote character
	bool TryParseSimpleCSV(DataChunk &insert_chunk, string &error_message);
	//! Sniffs CSV dialect and determines skip rows, header row, column types and column names
	vector<LogicalType> SniffCSV(const vector<LogicalType> &requested_types);

	//! Second phase of auto detection: detect candidate types for each column
	void DetectCandidateTypes(const vector<LogicalType> &type_candidates,
	                          const map<LogicalTypeId, vector<const char *>> &format_template_candidates,
	                          const vector<CSVReaderOptions> &info_candidates, CSVReaderOptions &original_options,
	                          idx_t best_num_cols, vector<vector<LogicalType>> &best_sql_types_candidates,
	                          std::map<LogicalTypeId, vector<string>> &best_format_candidates,
	                          DataChunk &best_header_row);
	//! Third phase of auto detection: detect header of CSV file
	void DetectHeader(const vector<vector<LogicalType>> &best_sql_types_candidates, const DataChunk &best_header_row);
	//! Fourth phase of auto detection: refine the types of each column and select which types to use for each column
	vector<LogicalType> RefineTypeDetection(const vector<LogicalType> &type_candidates,
	                                        const vector<LogicalType> &requested_types,
	                                        vector<vector<LogicalType>> &best_sql_types_candidates,
	                                        map<LogicalTypeId, vector<string>> &best_format_candidates);

	//! Skip Empty lines for tables with over one column
	void SkipEmptyLines();
};

} // namespace duckdb