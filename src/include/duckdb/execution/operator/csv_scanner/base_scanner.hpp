//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/base_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_error.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

class CSVFileScan;

//! Class that keeps track of line starts, used for line size verification
class LinePosition {
public:
	LinePosition() {
	}
	LinePosition(idx_t buffer_idx_p, idx_t buffer_pos_p, idx_t buffer_size_p)
	    : buffer_pos(buffer_pos_p), buffer_size(buffer_size_p), buffer_idx(buffer_idx_p) {
	}

	idx_t operator-(const LinePosition &other) const {
		if (other.buffer_idx == buffer_idx) {
			return buffer_pos - other.buffer_pos;
		}
		return other.buffer_size - other.buffer_pos + buffer_pos;
	}

	bool operator==(const LinePosition &other) const {
		return buffer_pos == other.buffer_pos && buffer_idx == other.buffer_idx && buffer_size == other.buffer_size;
	}

	idx_t GetGlobalPosition(idx_t requested_buffer_size, bool first_char_nl = false) const {
		return requested_buffer_size * buffer_idx + buffer_pos + first_char_nl;
	}
	idx_t buffer_pos = 0;
	idx_t buffer_size = 0;
	idx_t buffer_idx = 0;
};

class CurrentError {
public:
	CurrentError(CSVErrorType type, idx_t col_idx_p, idx_t chunk_idx_p, const LinePosition &error_position_p,
	             idx_t current_line_size_p)
	    : type(type), col_idx(col_idx_p), chunk_idx(chunk_idx_p), current_line_size(current_line_size_p),
	      error_position(error_position_p) {};
	//! Error Type (e.g., Cast, Wrong # of columns, ...)
	CSVErrorType type;
	//! Column index related to the CSV File columns
	idx_t col_idx;
	//! Column index related to the produced chunk (i.e., with projection applied)
	idx_t chunk_idx;
	//! Current CSV Line size in Bytes
	idx_t current_line_size;
	//! Error Message produced
	string error_message;
	//! Exact Position where the error happened
	LinePosition error_position;

	friend bool operator==(const CurrentError &error, CSVErrorType other) {
		return error.type == other;
	}
};

class ScannerResult;

class LineError {
public:
	explicit LineError(bool ignore_errors_p) : is_error_in_line(false), ignore_errors(ignore_errors_p) {};
	//! We clear up our CurrentError Vector
	void Reset() {
		current_errors.clear();
		is_error_in_line = false;
	}
	void Insert(const CSVErrorType &type, const idx_t &col_idx, const idx_t &chunk_idx,
	            const LinePosition &error_position, const idx_t current_line_size = 0) {
		is_error_in_line = true;
		if (!ignore_errors) {
			// We store it for later
			current_errors.push_back({type, col_idx, chunk_idx, error_position, current_line_size});
			current_errors.back().current_line_size = current_line_size;
		}
	}
	//! Set that we currently have an error, but don't really store them
	void SetError() {
		is_error_in_line = true;
	}
	//! Dirty hack for adding cast message
	void ModifyErrorMessageOfLastError(string error_message) {
		D_ASSERT(!current_errors.empty() && current_errors.back().type == CSVErrorType::CAST_ERROR);
		current_errors.back().error_message = std::move(error_message);
	}

	bool HasErrorType(CSVErrorType type) const {
		for (auto &error : current_errors) {
			if (type == error.type) {
				return true;
			}
		}
		return false;
	}

	bool HandleErrors(ScannerResult &result);

private:
	vector<CurrentError> current_errors;
	bool is_error_in_line;
	bool ignore_errors;
};

//! Keeps track of start and end of line positions in regard to the CSV file
class FullLinePosition {
public:
	FullLinePosition() {};
	LinePosition begin;
	LinePosition end;

	//! Reconstructs the current line to be used in error messages
	string ReconstructCurrentLine(bool &first_char_nl,
	                              unordered_map<idx_t, shared_ptr<CSVBufferHandle>> &buffer_handles,
	                              bool reconstruct_line) const;
};

class ScannerResult {
public:
	ScannerResult(CSVStates &states, CSVStateMachine &state_machine, idx_t result_size);

	//! Adds a Value to the result
	static inline void SetQuoted(ScannerResult &result, idx_t quoted_position) {
		if (!result.quoted) {
			result.quoted_position = quoted_position;
		}
		result.quoted = true;
	}
	static inline void SetEscaped(ScannerResult &result) {
		result.escaped = true;
	}
	static inline void SetComment(ScannerResult &result, idx_t buffer_pos) {
		result.comment = true;
	}
	static inline bool UnsetComment(ScannerResult &result, idx_t buffer_pos) {
		result.comment = false;
		return false;
	}
	static inline bool IsCommentSet(const ScannerResult &result) {
		return result.comment == true;
	}

	//! Variable to keep information regarding quoted and escaped values
	bool quoted = false;
	bool escaped = false;
	//! Variable to keep track if we are in a comment row. Hence, it won't add it
	bool comment = false;
	idx_t quoted_position = 0;
	idx_t chunk_col_id = 0;
	idx_t cur_col_id = 0;

	LinePosition last_position;
	idx_t buffer_size;

	//! Size of the result
	const idx_t result_size;

	idx_t number_of_rows = 0;

	//! Errors happening in the current line (if any)
	LineError current_errors;

	CSVStateMachine &state_machine;
	//! Line position of the current line
	FullLinePosition current_line_position;

	void Print() const {
		state_machine.Print();
	}

protected:
	CSVStates &states;
};

//! This is the base of our CSV scanners.
//! Scanners differ on what they are used for, and consequently have different performance benefits.
class BaseScanner {
public:
	explicit BaseScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine,
	                     shared_ptr<CSVErrorHandler> error_handler, bool sniffing = false,
	                     shared_ptr<CSVFileScan> csv_file_scan = nullptr, CSVIterator iterator = {});

	virtual ~BaseScanner() = default;

	//! Returns true if the scanner is finished
	bool FinishedFile();

	//! Parses data into an output_chunk
	virtual ScannerResult &ParseChunk();

	//! Returns the result from the last Parse call. Shouts at you if you call it wrong
	virtual ScannerResult &GetResult();

	CSVIterator &GetIterator();

	void SetIterator(const CSVIterator &it);

	idx_t GetBoundaryIndex() const {
		return iterator.GetBoundaryIdx();
	}

	idx_t GetLinesRead() const {
		return lines_read;
	}

	CSVPosition GetIteratorPosition() const {
		return iterator.pos;
	}

	CSVStateMachine &GetStateMachine() const;

	shared_ptr<CSVFileScan> csv_file_scan;

	//! If this scanner is being used for sniffing
	bool sniffing = false;
	//! The guy that handles errors
	shared_ptr<CSVErrorHandler> error_handler;

	//! Shared pointer to the state machine, this is used across multiple scanners
	shared_ptr<CSVStateMachine> state_machine;

	//! States
	CSVStates states;

	bool ever_quoted = false;

	//! Shared pointer to the buffer_manager, this is shared across multiple scanners
	shared_ptr<CSVBufferManager> buffer_manager;

	//! Skips Notes and/or parts of the data, starting from the top.
	//! notes are dirty lines on top of the file, before the actual data
	static CSVIterator SkipCSVRows(shared_ptr<CSVBufferManager> buffer_manager,
	                               const shared_ptr<CSVStateMachine> &state_machine, idx_t rows_to_skip);

protected:
	//! Boundaries of this scanner
	CSVIterator iterator;

	//! Unique pointer to the buffer_handle, this is unique per scanner, since it also contains the necessary counters
	//! To offload buffers to disk if necessary
	shared_ptr<CSVBufferHandle> cur_buffer_handle;

	//! Hold the current buffer ptr
	char *buffer_handle_ptr = nullptr;

	//! If this scanner has been initialized
	bool initialized = false;
	//! How many lines were read by this scanner
	idx_t lines_read = 0;
	idx_t bytes_read = 0;
	//! Internal Functions used to perform the parsing
	//! Initializes the scanner
	virtual void Initialize();

	void FindNewLine(ScannerResult &result);

	inline static bool ContainsZeroByte(uint64_t v) {
		return (v - UINT64_C(0x0101010101010101)) & ~(v)&UINT64_C(0x8080808080808080);
	}

	//! Process one chunk
	template <class T>
	void Process(T &result) {
		idx_t to_pos;
		const idx_t start_pos = iterator.pos.buffer_pos;
		if (iterator.IsBoundarySet()) {
			to_pos = iterator.GetEndPos();
			if (to_pos > cur_buffer_handle->actual_size) {
				to_pos = cur_buffer_handle->actual_size;
			}
		} else {
			to_pos = cur_buffer_handle->actual_size;
		}
		while (iterator.pos.buffer_pos < to_pos) {
			state_machine->Transition(states, buffer_handle_ptr[iterator.pos.buffer_pos]);
			switch (states.states[1]) {
			case CSVState::INVALID: {
				auto start_of_value = result.last_position;
				auto cur_col_id = result.cur_col_id;
				auto chunk_col_id = result.chunk_col_id;
				if (T::InvalidState(result)) {
					// if we got here, we have work to do
					// 1. Set buffer and position to where things went wrong
					// 2. Figure out new line
					FindNewLine(result);
					if (iterator.IsBoundarySet()) {
						to_pos = iterator.GetEndPos();
						if (to_pos > cur_buffer_handle->actual_size) {
							to_pos = cur_buffer_handle->actual_size;
						}
					} else {
						to_pos = cur_buffer_handle->actual_size;
					}
					result.current_line_position.begin = result.current_line_position.end;
					result.current_line_position.end = result.last_position;
					--result.current_line_position.end.buffer_pos;
					if (state_machine->dialect_options.state_machine_options.new_line.GetValue() ==
					    NewLineIdentifier::CARRY_ON) {
						--result.current_line_position.end.buffer_pos;
					}
					result.current_errors.Insert(UNTERMINATED_QUOTES, cur_col_id, chunk_col_id, start_of_value);
					result.current_errors.HandleErrors(result);
					++result.number_of_rows;
					bytes_read = iterator.pos.buffer_pos - start_pos;
				}
				lines_read++;
				if (result.number_of_rows >= result.result_size) {
					return;
				}
				break;
			}
			case CSVState::RECORD_SEPARATOR:
				if (states.states[0] == CSVState::RECORD_SEPARATOR || states.states[0] == CSVState::NOT_SET) {
					if (T::EmptyLine(result, iterator.pos.buffer_pos)) {
						iterator.pos.buffer_pos++;
						bytes_read = iterator.pos.buffer_pos - start_pos;
						lines_read++;
						return;
					}
					lines_read++;

				} else if (states.states[0] != CSVState::CARRIAGE_RETURN) {
					if (T::IsCommentSet(result)) {
						if (T::UnsetComment(result, iterator.pos.buffer_pos)) {
							iterator.pos.buffer_pos++;
							bytes_read = iterator.pos.buffer_pos - start_pos;
							lines_read++;
							return;
						}
					} else {
						if (T::AddRow(result, iterator.pos.buffer_pos)) {
							iterator.pos.buffer_pos++;
							bytes_read = iterator.pos.buffer_pos - start_pos;
							lines_read++;
							return;
						}
					}
					lines_read++;
				}
				iterator.pos.buffer_pos++;
				break;
			case CSVState::CARRIAGE_RETURN:
				if (states.states[0] == CSVState::RECORD_SEPARATOR || states.states[0] == CSVState::NOT_SET) {
					if (T::EmptyLine(result, iterator.pos.buffer_pos)) {
						iterator.pos.buffer_pos++;
						bytes_read = iterator.pos.buffer_pos - start_pos;
						lines_read++;
						return;
					}
				} else if (states.states[0] != CSVState::CARRIAGE_RETURN) {
					if (T::IsCommentSet(result)) {
						if (T::UnsetComment(result, iterator.pos.buffer_pos)) {
							iterator.pos.buffer_pos++;
							bytes_read = iterator.pos.buffer_pos - start_pos;
							lines_read++;
							return;
						}
					} else {
						if (T::AddRow(result, iterator.pos.buffer_pos)) {
							iterator.pos.buffer_pos++;
							bytes_read = iterator.pos.buffer_pos - start_pos;
							lines_read++;
							return;
						}
					}
				}
				iterator.pos.buffer_pos++;
				lines_read++;
				break;
			case CSVState::DELIMITER:
				T::AddValue(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				break;
			case CSVState::QUOTED: {
				if (states.states[0] == CSVState::UNQUOTED) {
					T::SetEscaped(result);
				}
				ever_quoted = true;
				T::SetQuoted(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				while (iterator.pos.buffer_pos + 8 < to_pos) {
					uint64_t value =
					    Load<uint64_t>(reinterpret_cast<const_data_ptr_t>(&buffer_handle_ptr[iterator.pos.buffer_pos]));
					if (ContainsZeroByte((value ^ state_machine->transition_array.quote) &
					                     (value ^ state_machine->transition_array.escape))) {
						break;
					}
					iterator.pos.buffer_pos += 8;
				}

				while (state_machine->transition_array
				           .skip_quoted[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
				       iterator.pos.buffer_pos < to_pos - 1) {
					iterator.pos.buffer_pos++;
				}
			} break;
			case CSVState::ESCAPE:
				T::SetEscaped(result);
				iterator.pos.buffer_pos++;
				break;
			case CSVState::STANDARD: {
				iterator.pos.buffer_pos++;
				while (iterator.pos.buffer_pos + 8 < to_pos) {
					uint64_t value =
					    Load<uint64_t>(reinterpret_cast<const_data_ptr_t>(&buffer_handle_ptr[iterator.pos.buffer_pos]));
					if (ContainsZeroByte((value ^ state_machine->transition_array.delimiter) &
					                     (value ^ state_machine->transition_array.new_line) &
					                     (value ^ state_machine->transition_array.carriage_return) &
					                     (value ^ state_machine->transition_array.comment))) {
						break;
					}
					iterator.pos.buffer_pos += 8;
				}
				while (state_machine->transition_array
				           .skip_standard[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
				       iterator.pos.buffer_pos < to_pos - 1) {
					iterator.pos.buffer_pos++;
				}
				break;
			}
			case CSVState::QUOTED_NEW_LINE:
				T::QuotedNewLine(result);
				iterator.pos.buffer_pos++;
				break;
			case CSVState::COMMENT: {
				T::SetComment(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				while (iterator.pos.buffer_pos + 8 < to_pos) {
					uint64_t value =
					    Load<uint64_t>(reinterpret_cast<const_data_ptr_t>(&buffer_handle_ptr[iterator.pos.buffer_pos]));
					if (ContainsZeroByte((value ^ state_machine->transition_array.new_line) &
					                     (value ^ state_machine->transition_array.carriage_return))) {
						break;
					}
					iterator.pos.buffer_pos += 8;
				}
				while (state_machine->transition_array
				           .skip_comment[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
				       iterator.pos.buffer_pos < to_pos - 1) {
					iterator.pos.buffer_pos++;
				}
				break;
			}
			default:
				iterator.pos.buffer_pos++;
				break;
			}
		}
		bytes_read = iterator.pos.buffer_pos - start_pos;
	}

	//! Finalizes the process of the chunk
	virtual void FinalizeChunkProcess();

	void SkipUntilNewLine();

	//! Internal function for parse chunk
	template <class T>
	void ParseChunkInternal(T &result) {
		if (!initialized) {
			Initialize();
			initialized = true;
		}
		if (!iterator.done && cur_buffer_handle) {
			Process(result);
		}
		FinalizeChunkProcess();
	}
};

} // namespace duckdb
