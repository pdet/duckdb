//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/string_value_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/base_scanner.hpp"

namespace duckdb {

struct CSVBufferUsage {
	CSVBufferUsage(CSVBufferManager &buffer_manager_p, idx_t buffer_idx_p)
	    : buffer_manager(buffer_manager_p), buffer_idx(buffer_idx_p) {

	                                        };
	~CSVBufferUsage() {
		buffer_manager.ResetBuffer(buffer_idx);
	}
	CSVBufferManager &buffer_manager;
	idx_t buffer_idx;
};

struct ParseTypeInfo {
	ParseTypeInfo() {};
	ParseTypeInfo(const LogicalType &type, bool validate_utf_8_p) : validate_utf8(validate_utf_8_p) {
		type_id = type.id();
		internal_type = type.InternalType();
		if (type.id() == LogicalTypeId::DECIMAL) {
			// We only care about these if we have a decimal value
			type.GetDecimalProperties(width, scale);
		}
	}

	bool validate_utf8;
	LogicalTypeId type_id;
	PhysicalType internal_type;
	uint8_t scale;
	uint8_t width;
};
class StringValueResult : public ScannerResult {
public:
	StringValueResult(CSVStates &states, CSVStateMachine &state_machine,
	                  const shared_ptr<CSVBufferHandle> &buffer_handle, Allocator &buffer_allocator,
	                  idx_t result_size_p, idx_t buffer_position, CSVErrorHandler &error_handler, CSVIterator &iterator,
	                  bool store_line_size, shared_ptr<CSVFileScan> csv_file_scan, idx_t &lines_read, bool sniffing,
	                  string path);

	~StringValueResult();

	//! Information on the vector
	unsafe_vector<void *> vector_ptr;
	unsafe_vector<ValidityMask *> validity_mask;

	//! Variables to iterate over the CSV buffers
	char *buffer_ptr;
	idx_t position_before_comment;

	//! CSV Options that impact the parsing
	const uint32_t number_of_columns;
	const bool null_padding;
	const bool ignore_errors;

	unsafe_unique_array<const char *> null_str_ptr;
	unsafe_unique_array<idx_t> null_str_size;
	idx_t null_str_count;

	//! Internal Data Chunk used for flushing
	DataChunk parse_chunk;
	idx_t cur_col_id = 0;
	bool figure_out_new_line = false;
	//! Information to properly handle errors
	CSVErrorHandler &error_handler;
	CSVIterator &iterator;
	//! Used for CSV line reconstruction on flushed errors
	unordered_map<idx_t, FullLinePosition> line_positions_per_row;
	bool store_line_size = false;
	bool added_last_line = false;
	bool quoted_new_line = false;

	unsafe_unique_array<ParseTypeInfo> parse_types;
	vector<string> names;

	shared_ptr<CSVFileScan> csv_file_scan;
	idx_t &lines_read;
	//! Information regarding projected columns
	unsafe_unique_array<bool> projected_columns;
	bool projecting_columns = false;
	idx_t chunk_col_id = 0;

	//! We must ensure that we keep the buffers alive until processing the query result
	unordered_map<idx_t, shared_ptr<CSVBufferHandle>> buffer_handles;

	//! Requested size of buffers (i.e., either 32Mb or set by buffer_size parameter)
	idx_t requested_size;

	StrpTimeFormat date_format, timestamp_format;
	bool sniffing;

	char decimal_separator;

	//! We store borked rows so we can generate multiple errors during flushing
	unordered_set<idx_t> borked_rows;

	const string path;

	//! Variable used when trying to figure out where a new segment starts, we must always start from a Valid
	//! (i.e., non-comment) line.
	bool first_line_is_comment = false;

	//! Specialized code for quoted values, makes sure to remove quotes and escapes
	static inline void AddQuotedValue(StringValueResult &result, const idx_t buffer_pos);
	//! Adds a Value to the result
	static inline void AddValue(StringValueResult &result, const idx_t buffer_pos);
	//! Adds a Row to the result
	static inline bool AddRow(StringValueResult &result, const idx_t buffer_pos);
	//! Behavior when hitting an invalid state
	static inline bool InvalidState(StringValueResult &result);
	//! Handles QuotedNewline State
	static inline void QuotedNewLine(StringValueResult &result);
	void NullPaddingQuotedNewlineCheck() const;
	//! Handles EmptyLine states
	static inline bool EmptyLine(StringValueResult &result, const idx_t buffer_pos);
	inline bool AddRowInternal();
	//! Force the throw of a unicode error
	void HandleUnicodeError(idx_t col_idx, LinePosition &error_position);
	bool HandleTooManyColumnsError(const char *value_ptr, const idx_t size);
	inline void AddValueToVector(const char *value_ptr, const idx_t size, bool allocate = false);
	static inline void SetComment(StringValueResult &result, idx_t buffer_pos);
	static inline bool UnsetComment(StringValueResult &result, idx_t buffer_pos);

	DataChunk &ToChunk();
	//! Resets the state of the result
	void Reset();

	//! BOM skipping (https://en.wikipedia.org/wiki/Byte_order_mark)
	void SkipBOM() const;
	//! If we should Print Error Lines
	//! We only really care about error lines if we are going to error or store them in a rejects table
	bool PrintErrorLine() const;
	//! Removes last added line, usually because we figured out later on that it's an ill-formed line
	//! or that it does not fit our schema
	void RemoveLastLine();
};

//! Our dialect scanner basically goes over the CSV and actually parses the values to a DuckDB vector of string_t
class StringValueScanner : public BaseScanner {
public:
	StringValueScanner(idx_t scanner_idx, const shared_ptr<CSVBufferManager> &buffer_manager,
	                   const shared_ptr<CSVStateMachine> &state_machine,
	                   const shared_ptr<CSVErrorHandler> &error_handler, const shared_ptr<CSVFileScan> &csv_file_scan,
	                   bool sniffing = false, const CSVIterator &boundary = {},
	                   idx_t result_size = STANDARD_VECTOR_SIZE);

	StringValueScanner(const shared_ptr<CSVBufferManager> &buffer_manager,
	                   const shared_ptr<CSVStateMachine> &state_machine,
	                   const shared_ptr<CSVErrorHandler> &error_handler, idx_t result_size = STANDARD_VECTOR_SIZE,
	                   const CSVIterator &boundary = {});

	StringValueResult &ParseChunk() override;

	//! Flushes the result to the insert_chunk
	void Flush(DataChunk &insert_chunk);

	//! Function that creates and returns a non-boundary CSV Scanner, can be used for internal csv reading.
	static unique_ptr<StringValueScanner> GetCSVScanner(ClientContext &context, CSVReaderOptions &options);

	bool FinishedIterator() const;

	//! Creates a new string with all escaped values removed
	static string_t RemoveEscape(const char *str_ptr, idx_t end, char escape, Vector &vector);

	//! If we can directly cast the type when consuming the CSV file, or we have to do it later
	static bool CanDirectlyCast(const LogicalType &type, bool icu_loaded);

	const idx_t scanner_idx;

	//! Variable that manages buffer tracking
	shared_ptr<CSVBufferUsage> buffer_tracker;

	StringValueResult result;
	vector<LogicalType> types;

	//! Pointer to the previous buffer handle, necessary for overbuffer values
	shared_ptr<CSVBufferHandle> previous_buffer_handle;

private:
	void Initialize() override;

	void FinalizeChunkProcess() override;

	//! Function used to process values that go over the first buffer, extra allocation might be necessary
	void ProcessOverbufferValue();

	void ProcessExtraRow();
	//! Function used to move from one buffer to the other, if necessary
	bool MoveToNextBuffer();

	void SetStart();
};

} // namespace duckdb
