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
#include "duckdb/execution/operator/csv_scanner/csv_structural_cursor.hpp"

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

class ScannerResult {
public:
	ScannerResult(CSVStates &states, CSVStateMachine &state_machine, idx_t result_size);

	static inline void SetQuoted(ScannerResult &result, idx_t quoted_position) {
		if (!result.quoted) {
			result.quoted_position = quoted_position;
		}
		result.quoted = true;
		result.unquoted = true;
	}

	static inline void SetUnquoted(ScannerResult &result) {
		if (result.states.states[0] == CSVState::UNQUOTED && result.states.states[1] == CSVState::UNQUOTED &&
		    result.state_machine.dialect_options.state_machine_options.escape != '\0') {
			// This means we touched an unescaped quote, we must go through the remove escape code to remove it.
			result.escaped = true;
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

	inline bool IsStateCurrent(CSVState state) const {
		return states.states[1] == state;
	}

	//! Variable to keep information regarding quoted and escaped values
	bool quoted = false;
	//! If the current quoted value is unquoted
	bool unquoted = false;
	//! If the current value has been escaped
	bool escaped = false;
	//! Variable to keep track if we are in a comment row. Hence, won't add it
	bool comment = false;
	//! Whether the value being added is known to be ASCII, so its unicode validation can be skipped
	bool field_is_ascii = false;
	idx_t quoted_position = 0;

	LinePosition last_position;

	//! Size of the result
	const idx_t result_size;

	CSVStateMachine &state_machine;
	bool cur_line_starts_as_comment = false;

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
	                     shared_ptr<CSVFileScan> csv_file_scan = nullptr, const CSVIterator &iterator = {});

	virtual ~BaseScanner() = default;

	void Print() const;

	//! Returns true if the scanner is finished
	bool FinishedFile() const;

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

	//! Removes thousands separator
	static string RemoveSeparator(const char *value_ptr, const idx_t size, char thousands_separator);

	shared_ptr<CSVFileScan> csv_file_scan;

	//! If this scanner is being used for sniffing
	bool sniffing = false;
	//! The guy that handles errors
	shared_ptr<CSVErrorHandler> error_handler;

	//! Shared pointer to the state machine, this is used across multiple scanners
	shared_ptr<CSVStateMachine> state_machine;

	//! States
	CSVStates states;

	//! If the scanner ever entered a quoted state
	bool ever_quoted = false;

	//! If the scanner ever entered an escaped state.
	bool ever_escaped = false;

	//! If the scanner ever used advantage of the non-strict mode.
	bool used_unstrictness = false;

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
	const char *buffer_handle_ptr = nullptr;

	//! If this scanner has been initialized
	bool initialized = false;
	//! How many lines were read by this scanner
	idx_t lines_read = 0;
	idx_t bytes_read = 0;
	//! Internal Functions used to perform the parsing
	//! Initializes the scanner
	virtual void Initialize();

	//! Finds the structural bytes of the current buffer, mutable because the line finder is const
	mutable CSVStructuralCursor cursor;

	//! The structural bytes of the dialect, resolved once for the skip block and the row walker
	struct StructuralDialect {
		char delimiter = '\0';
		bool one_byte_delimiter = false;
		//! Whether quoting is enabled, the quote byte only means something then
		bool has_quote = false;
		char quote = '\0';
		//! Whether a quote inside quoted content is escaped by doubling it
		bool doubled_quotes = false;
		bool has_comment = false;
		char comment = '\0';
		//! An escape byte other than the quote, only the byte loop handles it
		bool has_distinct_escape = false;
		char escape = '\0';
		bool strict = false;
		bool carry_on = false;
	};
	StructuralDialect dialect;

	//! Resolves the dialect and the cursor patterns from the state machine options
	void ResolveDialect() {
		const auto &options = state_machine->state_machine_options;
		const auto &delimiter = options.delimiter.GetValue();
		dialect.delimiter = delimiter.empty() ? '\0' : delimiter[0];
		dialect.one_byte_delimiter = delimiter.size() == 1;
		dialect.quote = options.quote.GetValue();
		dialect.has_quote = dialect.quote != '\0';
		dialect.escape = options.escape.GetValue();
		dialect.doubled_quotes = dialect.has_quote && dialect.escape == dialect.quote;
		dialect.has_distinct_escape = dialect.escape != '\0' && dialect.escape != dialect.quote;
		dialect.comment = options.comment.GetValue();
		dialect.has_comment = dialect.comment != '\0';
		dialect.strict = options.strict_mode.GetValue();
		dialect.carry_on = options.new_line.GetValue() == NewLineIdentifier::CARRY_ON;
		cursor.AddPattern(static_cast<uint8_t>(dialect.delimiter), 0xff);
		if (dialect.has_quote) {
			cursor.AddPattern(static_cast<uint8_t>(dialect.quote), 0xff);
		}
		// the byte class holding \n and \r
		cursor.AddPattern(0x08, 0xf8);
		if (dialect.has_comment) {
			cursor.AddPattern(static_cast<uint8_t>(dialect.comment), 0xff);
		}
		if (dialect.has_distinct_escape) {
			cursor.AddPattern(static_cast<uint8_t>(dialect.escape), 0xff);
		}
	}

	//! Whether strict \r\n rows are set by the user, the byte loop then adds rows at the \n only
	bool OnlyCarriageReturnNewlines() const {
		const auto &options = state_machine->state_machine_options;
		return options.strict_mode.GetValue() && options.strict_mode.IsSetByUser() &&
		       options.new_line.GetValue() == NewLineIdentifier::CARRY_ON && options.new_line.IsSetByUser();
	}

	//! Binds the cursor to the current buffer
	void BindCursor() const {
		cursor.Bind(cur_buffer_handle->buffer_idx, buffer_handle_ptr, cur_buffer_handle->actual_size);
	}

	//! Whether ProcessPlainRows drives the rows of this scan
	bool use_plain_rows = false;

	//! Whether ProcessPlainRows fits the dialect, one byte delimiter, no comment, escape is the quote or none
	bool PlainRowsApplicable() const {
		const auto new_line = state_machine->state_machine_options.new_line.GetValue();
		return !dialect.has_comment && dialect.one_byte_delimiter && dialect.delimiter != ' ' &&
		       !dialect.has_distinct_escape &&
		       (new_line == NewLineIdentifier::SINGLE_N || new_line == NewLineIdentifier::CARRY_ON) &&
		       !OnlyCarriageReturnNewlines();
	}

	//! The byte loop state after the run [run_start, pos) from `cur`, one space is EMPTY_SPACE, else STANDARD
	static inline CSVState AfterRun(const CSVState cur, const char *buffer, const idx_t run_start, const idx_t pos) {
		if (pos == run_start) {
			return cur;
		}
		if (cur == CSVState::STANDARD || cur == CSVState::EMPTY_SPACE || buffer[run_start] != ' ' ||
		    pos - run_start > 1) {
			return CSVState::STANDARD;
		}
		return CSVState::EMPTY_SPACE;
	}

	enum class RowStep : uint8_t { CONTINUE, FINISHED, HAND_OVER };

	//! Leaves the byte at `pos` to the byte loop, in state `current`
	inline RowStep HandOver(const CSVState current, const idx_t pos) {
		states.states[1] = current;
		iterator.pos.buffer_pos = pos;
		return RowStep::HAND_OVER;
	}

	//! Whether the value that starts at the last position lies in ASCII only blocks
	template <class T>
	bool ValueIsAscii(const T &result) const {
		return cursor.AsciiFrom(result.last_position.buffer_pos);
	}

	//! The delimiter at `pos` from state `before`, with the value callback of the byte loop
	template <class T>
	void ValueEnd(T &result, const CSVState before, idx_t &pos) {
		states.states[0] = before;
		states.states[1] = CSVState::DELIMITER;
		iterator.pos.buffer_pos = pos;
		result.field_is_ascii = ValueIsAscii(result);
		T::AddValue(result, pos);
		result.field_is_ascii = false;
		pos++;
	}

	//! The quote at `pos` from state `before` opens quoted content, with the quote callback of the byte loop
	template <class T>
	void QuoteOpened(T &result, const CSVState before, idx_t &pos) {
		states.states[0] = before;
		states.states[1] = CSVState::QUOTED;
		ever_quoted = true;
		iterator.pos.buffer_pos = pos;
		T::SetQuoted(result, pos);
		pos++;
	}

	//! The row end at `pos` from state `before` with the row callback of the byte loop, on CONTINUE `pos` is past it
	template <class T>
	RowStep RowEnd(T &result, const idx_t to_pos, const CSVState before, idx_t &pos) {
		const bool carriage_return = buffer_handle_ptr[pos] == '\r';
		// a line ending that does not fit the dialect, or a \r without its \n in this scan, is left to the byte loop
		if (carriage_return != dialect.carry_on ||
		    (carriage_return && (pos + 1 >= to_pos || buffer_handle_ptr[pos + 1] != '\n'))) {
			return HandOver(before, pos);
		}
		states.states[0] = before;
		states.states[1] = carriage_return ? CSVState::CARRIAGE_RETURN : CSVState::RECORD_SEPARATOR;
		iterator.pos.buffer_pos = pos;
		result.field_is_ascii = ValueIsAscii(result);
		const bool full = T::AddRow(result, pos);
		result.field_is_ascii = false;
		pos++;
		lines_read++;
		if (full) {
			iterator.pos.buffer_pos = pos;
			return RowStep::FINISHED;
		}
		if (carriage_return) {
			// the \n after the \r has no callbacks
			states.states[0] = CSVState::CARRIAGE_RETURN;
			states.states[1] = CSVState::RECORD_SEPARATOR;
			pos++;
		}
		return RowStep::CONTINUE;
	}

	//! A quoted field from after its opening quote to past what closes it, strict mode with the quote as escape
	template <class T>
	RowStep QuotedField(T &result, const idx_t to_pos, idx_t &pos) {
		const char *buffer = buffer_handle_ptr;
		const char quote = dialect.quote;
		while (true) {
			// quoted content up to a quote or a line break, the byte loop skips everything else as well
			const auto stop = cursor.AdvanceToStop(to_pos, pos);
			if (stop == CSVStructuralCursor::Stop::LIMIT) {
				states.states[1] = CSVState::QUOTED;
				iterator.pos.buffer_pos = pos;
				return RowStep::FINISHED;
			}
			if (stop == CSVStructuralCursor::Stop::TAIL) {
				return HandOver(CSVState::QUOTED, pos);
			}
			char byte = buffer[pos];
			CSVState before = CSVState::QUOTED;
			if (byte == '\n' || byte == '\r') {
				states.states[0] = CSVState::QUOTED;
				states.states[1] = CSVState::QUOTED_NEW_LINE;
				T::QuotedNewLine(result);
				pos++;
				if (pos >= to_pos) {
					iterator.pos.buffer_pos = pos;
					return RowStep::FINISHED;
				}
				byte = buffer[pos];
				if (byte != quote) {
					QuoteOpened(result, CSVState::QUOTED_NEW_LINE, pos);
					continue;
				}
				before = CSVState::QUOTED_NEW_LINE;
			} else if (byte != quote) {
				// a delimiter or another flagged byte inside the quotes is content
				pos++;
				continue;
			}
			// the closing quote
			states.states[0] = before;
			states.states[1] = CSVState::UNQUOTED;
			T::SetUnquoted(result);
			pos++;
			// after it spaces are tolerated, a quote is an escaped quote, a delimiter or line ending closes the field
			while (true) {
				if (pos >= to_pos) {
					iterator.pos.buffer_pos = pos;
					return RowStep::FINISHED;
				}
				byte = buffer[pos];
				if (byte == dialect.delimiter) {
					ValueEnd(result, CSVState::UNQUOTED, pos);
					return RowStep::CONTINUE;
				}
				if (byte == '\n' || byte == '\r') {
					return RowEnd(result, to_pos, CSVState::UNQUOTED, pos);
				}
				if (byte == quote) {
					if (!dialect.doubled_quotes) {
						return HandOver(CSVState::UNQUOTED, pos);
					}
					ever_escaped = true;
					T::SetEscaped(result);
					QuoteOpened(result, CSVState::UNQUOTED, pos);
					break;
				}
				if (byte == ' ') {
					states.states[0] = CSVState::UNQUOTED;
					states.states[1] = CSVState::UNQUOTED;
					T::SetUnquoted(result);
					pos++;
					continue;
				}
				// anything else is invalid here, the byte loop reports it
				return HandOver(CSVState::UNQUOTED, pos);
			}
		}
	}

	//! Walks rows from the skip block with the byte loop's callbacks, handing over at the first byte it does not model
	template <class T>
	bool ProcessPlainRows(T &result, const idx_t to_pos) {
		CSVState cur = states.states[1];
		switch (cur) {
		case CSVState::STANDARD:
		case CSVState::DELIMITER:
		case CSVState::RECORD_SEPARATOR:
		case CSVState::NOT_SET:
		case CSVState::EMPTY_SPACE:
			break;
		default:
			return false;
		}
		const char *buffer = buffer_handle_ptr;
		idx_t pos = iterator.pos.buffer_pos;
		idx_t run_start = pos;
		while (true) {
			const auto stop = cursor.AdvanceToStop(to_pos, pos);
			const CSVState after_run = AfterRun(cur, buffer, run_start, pos);
			if (stop != CSVStructuralCursor::Stop::FOUND) {
				states.states[1] = after_run;
				iterator.pos.buffer_pos = pos;
				return stop == CSVStructuralCursor::Stop::LIMIT;
			}
			const char byte = buffer[pos];
			RowStep step = RowStep::CONTINUE;
			if (byte == dialect.delimiter) {
				ValueEnd(result, after_run, pos);
			} else if (byte == '\n' || byte == '\r') {
				if (pos == run_start && (cur == CSVState::RECORD_SEPARATOR || cur == CSVState::NOT_SET)) {
					// an empty row depends on the mode, the byte loop decides
					step = HandOver(after_run, pos);
				} else {
					step = RowEnd(result, to_pos, after_run, pos);
				}
			} else if (dialect.has_quote && byte == dialect.quote && after_run != CSVState::STANDARD) {
				// a quote opens quoted content here, inside a field it is content
				if (!dialect.strict) {
					step = HandOver(after_run, pos);
				} else {
					QuoteOpened(result, after_run, pos);
					step = QuotedField(result, to_pos, pos);
				}
			} else {
				// a flagged byte that is field content
				pos++;
				continue;
			}
			if (step != RowStep::CONTINUE) {
				return step == RowStep::FINISHED;
			}
			cur = states.states[1];
			run_start = pos;
		}
	}

	//! Process one chunk
	template <class T>
	void Process(T &result) {
		idx_t to_pos;
		const bool has_escaped_value = state_machine->dialect_options.state_machine_options.escape != '\0';
		const bool only_rn_newlines = OnlyCarriageReturnNewlines();
		const idx_t start_pos = iterator.pos.buffer_pos;
		if (iterator.IsBoundarySet()) {
			to_pos = iterator.GetEndPos();
			if (to_pos > cur_buffer_handle->actual_size) {
				to_pos = cur_buffer_handle->actual_size;
			}
		} else {
			to_pos = cur_buffer_handle->actual_size;
		}
		BindCursor();
		while (iterator.pos.buffer_pos < to_pos) {
			if (use_plain_rows && ProcessPlainRows(result, to_pos)) {
				break;
			}
			// the byte loop takes the row the plain path could not, and hands back after it
			bool row_done = false;
			while (iterator.pos.buffer_pos < to_pos && !row_done) {
				state_machine->Transition(states, buffer_handle_ptr[iterator.pos.buffer_pos]);
				switch (states.states[1]) {
				case CSVState::INVALID:
					T::InvalidState(result);
					iterator.pos.buffer_pos++;
					bytes_read = iterator.pos.buffer_pos - start_pos;
					return;
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
					row_done = use_plain_rows;
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
						} else if (!only_rn_newlines) {
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
					if ((states.states[0] == CSVState::UNQUOTED || states.states[0] == CSVState::MAYBE_QUOTED) &&
					    has_escaped_value) {
						ever_escaped = true;
						T::SetEscaped(result);
					}
					if ((states.states[0] == CSVState::ESCAPE || states.states[0] == CSVState::ESCAPED_RETURN ||
					     states.states[0] == CSVState::UNQUOTED_ESCAPE) &&
					    (buffer_handle_ptr[iterator.pos.buffer_pos] ==
					         state_machine->dialect_options.state_machine_options.quote.GetValue() ||
					     !state_machine->dialect_options.state_machine_options.strict_mode.GetValue())) {
						// We only set the ever escaped variable if this is either a quote char OR strict mode is off
						ever_escaped = true;
						if (states.states[0] == CSVState::UNQUOTED_ESCAPE) {
							used_unstrictness = true;
						}
					}
					ever_quoted = true;
					T::SetQuoted(result, iterator.pos.buffer_pos);
					iterator.pos.buffer_pos++;
					cursor.SkipUntilStop(state_machine->transition_array.skip_quoted, to_pos, iterator.pos.buffer_pos);
				} break;
				case CSVState::UNQUOTED: {
					if (states.states[0] == CSVState::MAYBE_QUOTED) {
						ever_escaped = true;
						T::SetEscaped(result);
					}
					T::SetUnquoted(result);
					iterator.pos.buffer_pos++;
					break;
				}
				case CSVState::ESCAPE:
				case CSVState::ESCAPED_RETURN:
					T::SetEscaped(result);
					iterator.pos.buffer_pos++;
					break;
				case CSVState::UNQUOTED_ESCAPE:
					T::SetEscaped(result);
					iterator.pos.buffer_pos++;
					used_unstrictness = true;
					break;
				case CSVState::STANDARD:
					iterator.pos.buffer_pos++;
					cursor.SkipUntilStop(state_machine->transition_array.skip_standard, to_pos,
					                     iterator.pos.buffer_pos);
					break;
				case CSVState::QUOTED_NEW_LINE:
					T::QuotedNewLine(result);
					iterator.pos.buffer_pos++;
					break;
				case CSVState::COMMENT:
					T::SetComment(result, iterator.pos.buffer_pos);
					iterator.pos.buffer_pos++;
					cursor.SkipUntilStop(state_machine->transition_array.skip_comment, to_pos, iterator.pos.buffer_pos);
					break;
				default:
					iterator.pos.buffer_pos++;
					break;
				}
			}
		}
		bytes_read = iterator.pos.buffer_pos - start_pos;
	}

	//! Finalizes the process of the chunk
	virtual void FinalizeChunkProcess();

	//! Internal function for parse chunk
	template <class T>
	void ParseChunkInternal(T &result) {
		if (iterator.done) {
			return;
		}
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
