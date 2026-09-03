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
#include "duckdb/common/swar.hpp"

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
	//! Whether the value being added is known to be ASCII, so its UTF-8 validation can be skipped
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

	//! Skip-stop mask of one 64-byte block [start, end) of the current buffer, bit i is byte start + i
	struct SkipBlock {
		//! The buffer the block belongs to and its size
		const char *buffer_ptr = nullptr;
		idx_t buffer_size = 0;
		idx_t start = 0;
		idx_t end = 0;
		//! A superset of the bytes that end a skip in any state
		uint64_t stops = 0;
		//! The start of the run of ASCII-only blocks that ends with this block, invalid if it is not ASCII
		idx_t ascii_start = DConstants::INVALID_INDEX;
		//! The byte patterns that end a skip: delimiter, quote, the class of \n and \r, and comment and escape if set
		vector<SwarBlock::BytePattern> patterns;
	};
	SkipBlock skip_block;

	//! Binds the skip block to the current buffer, dropping the mask of a previous one
	void BindSkipBlock() {
		if (skip_block.patterns.empty()) {
			const auto &options = state_machine->state_machine_options;
			const auto &delimiter = options.delimiter.GetValue();
			const auto quote = static_cast<uint8_t>(options.quote.GetValue());
			const auto escape = static_cast<uint8_t>(options.escape.GetValue());
			const auto comment = static_cast<uint8_t>(options.comment.GetValue());
			skip_block.patterns.emplace_back(delimiter.empty() ? uint8_t(0) : static_cast<uint8_t>(delimiter[0]), 0xff);
			skip_block.patterns.emplace_back(quote, 0xff);
			skip_block.patterns.emplace_back(0x08, 0xf8);
			if (comment != '\0') {
				skip_block.patterns.emplace_back(comment, 0xff);
			}
			if (escape != '\0' && escape != quote) {
				skip_block.patterns.emplace_back(escape, 0xff);
			}
		}
		if (skip_block.buffer_ptr == buffer_handle_ptr) {
			return;
		}
		skip_block.buffer_ptr = buffer_handle_ptr;
		skip_block.buffer_size = cur_buffer_handle->actual_size;
		skip_block.start = 0;
		skip_block.end = 0;
		skip_block.ascii_start = DConstants::INVALID_INDEX;
	}

	//! Computes the skip-stop mask of the block that starts at `start`
	void LoadSkipBlock(idx_t start) {
		const char *block = buffer_handle_ptr + start;
		const auto *patterns = skip_block.patterns.data();
		switch (skip_block.patterns.size()) {
		case 3:
			skip_block.stops = SwarBlock::MaybeAnyMask<3>(block, patterns);
			break;
		case 4:
			skip_block.stops = SwarBlock::MaybeAnyMask<4>(block, patterns);
			break;
		default:
			skip_block.stops = SwarBlock::MaybeAnyMask<5>(block, patterns);
			break;
		}
		if (!SwarBlock::IsAscii(block)) {
			skip_block.ascii_start = DConstants::INVALID_INDEX;
		} else if (skip_block.ascii_start == DConstants::INVALID_INDEX || skip_block.end != start) {
			skip_block.ascii_start = start;
		}
		skip_block.start = start;
		skip_block.end = start + SwarBlock::SIZE;
	}

	enum class StopResult : uint8_t { FOUND, LIMIT, TAIL };

	//! Moves `pos` over the bytes not flagged in the skip block, up to `limit`
	//! FOUND: at a flagged byte, LIMIT: at `limit`, TAIL: fewer than a block of bytes remain in the buffer
	inline StopResult AdvanceToStop(const idx_t limit, idx_t &pos) {
		while (pos < limit) {
			if (pos < skip_block.start || pos >= skip_block.end) {
				if (pos + SwarBlock::SIZE > skip_block.buffer_size) {
					return StopResult::TAIL;
				}
				LoadSkipBlock(pos);
			}
			const uint64_t remaining = skip_block.stops >> (pos - skip_block.start);
			if (remaining) {
				const idx_t stop = pos + CountZeros<uint64_t>::Trailing(remaining);
				if (stop >= limit) {
					break;
				}
				pos = stop;
				return StopResult::FOUND;
			}
			pos = MinValue<idx_t>(skip_block.end, limit);
		}
		pos = limit;
		return StopResult::LIMIT;
	}

	//! Moves the position to the next byte that `skip_table` does not skip, or to `to_pos - 1`
	//! The skip block flags a superset of those bytes, so the walk only inspects the flagged ones
	inline void SkipUntilStop(const bool *skip_table, const idx_t to_pos) {
		auto &pos = iterator.pos.buffer_pos;
		while (pos + 1 < to_pos) {
			const auto stop = AdvanceToStop(to_pos - 1, pos);
			if (stop == StopResult::TAIL) {
				// the tail of the buffer is walked byte by byte
				while (skip_table[static_cast<uint8_t>(buffer_handle_ptr[pos])] && pos + 1 < to_pos) {
					pos++;
				}
				return;
			}
			if (stop == StopResult::LIMIT || !skip_table[static_cast<uint8_t>(buffer_handle_ptr[pos])]) {
				return;
			}
			// a flagged byte the skip table skips after all
			pos++;
		}
	}

	//! Whether ProcessPlainRows drives the rows of this scan
	bool use_plain_rows = false;

	//! The dialects ProcessPlainRows models outside quotes: no comment, a one-byte delimiter that is not a space, a
	//! quote that is its own escape or has none, and \n or \r\n rows
	bool PlainRowsApplicable() const {
		const auto &options = state_machine->state_machine_options;
		const auto &delimiter = options.delimiter.GetValue();
		const char quote = options.quote.GetValue();
		const char escape = options.escape.GetValue();
		const auto new_line = options.new_line.GetValue();
		const bool only_rn_newlines = options.strict_mode.GetValue() && options.strict_mode.IsSetByUser() &&
		                              new_line == NewLineIdentifier::CARRY_ON && options.new_line.IsSetByUser();
		return options.comment.GetValue() == '\0' && delimiter.size() == 1 && delimiter[0] != ' ' &&
		       (escape == quote || escape == '\0') &&
		       (new_line == NewLineIdentifier::SINGLE_N || new_line == NewLineIdentifier::CARRY_ON) &&
		       !only_rn_newlines;
	}

	//! The state the byte loop is in after the field bytes [run_start, pos) from state `cur`: the first byte moves
	//! it, a single space to EMPTY_SPACE and anything else to STANDARD, the rest are skipped
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

	//! The row end at `pos` (a \n or \r) from state `before`, with the row callback of the byte loop
	//! On CONTINUE `pos` is past the line ending and `states` at RECORD_SEPARATOR
	template <class T>
	RowStep RowEnd(T &result, const idx_t to_pos, const bool carry_on, const CSVState before, idx_t &pos) {
		const bool carriage_return = buffer_handle_ptr[pos] == '\r';
		// a line ending that does not match the dialect depends on the mode, the byte loop decides; a \r must be
		// followed by its \n within this scan
		if (carriage_return != carry_on ||
		    (carriage_return && (pos + 1 >= to_pos || buffer_handle_ptr[pos + 1] != '\n'))) {
			states.states[1] = before;
			iterator.pos.buffer_pos = pos;
			return RowStep::HAND_OVER;
		}
		states.states[0] = before;
		states.states[1] = carriage_return ? CSVState::CARRIAGE_RETURN : CSVState::RECORD_SEPARATOR;
		iterator.pos.buffer_pos = pos;
		result.field_is_ascii = skip_block.ascii_start <= result.last_position.buffer_pos;
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

	//! A quoted field from the byte after its opening quote to past the delimiter or line ending that closes it,
	//! with the quote callbacks of the byte loop. Strict mode only: a quote is its own escape or there is none
	template <class T>
	RowStep QuotedField(T &result, const idx_t to_pos, const char delimiter, const char quote,
	                    const bool escaped_quotes, const bool carry_on, idx_t &pos) {
		const char *buffer = buffer_handle_ptr;
		while (true) {
			// quoted content up to a quote or a line break, the byte loop skips everything else as well
			const auto stop = AdvanceToStop(to_pos, pos);
			if (stop != StopResult::FOUND) {
				states.states[1] = CSVState::QUOTED;
				iterator.pos.buffer_pos = pos;
				return stop == StopResult::LIMIT ? RowStep::FINISHED : RowStep::HAND_OVER;
			}
			char byte = buffer[pos];
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
					states.states[0] = CSVState::QUOTED_NEW_LINE;
					states.states[1] = CSVState::QUOTED;
					ever_quoted = true;
					iterator.pos.buffer_pos = pos;
					T::SetQuoted(result, pos);
					pos++;
					continue;
				}
				states.states[0] = CSVState::QUOTED_NEW_LINE;
			} else if (byte != quote) {
				// a delimiter or another flagged byte inside the quotes is content
				pos++;
				continue;
			} else {
				states.states[0] = CSVState::QUOTED;
			}
			// the closing quote
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
				if (byte == delimiter) {
					states.states[0] = CSVState::UNQUOTED;
					states.states[1] = CSVState::DELIMITER;
					iterator.pos.buffer_pos = pos;
					result.field_is_ascii = skip_block.ascii_start <= result.last_position.buffer_pos;
					T::AddValue(result, pos);
					result.field_is_ascii = false;
					pos++;
					return RowStep::CONTINUE;
				}
				if (byte == '\n' || byte == '\r') {
					return RowEnd(result, to_pos, carry_on, CSVState::UNQUOTED, pos);
				}
				if (byte == quote) {
					if (!escaped_quotes) {
						iterator.pos.buffer_pos = pos;
						return RowStep::HAND_OVER;
					}
					states.states[0] = CSVState::UNQUOTED;
					states.states[1] = CSVState::QUOTED;
					ever_escaped = true;
					T::SetEscaped(result);
					ever_quoted = true;
					iterator.pos.buffer_pos = pos;
					T::SetQuoted(result, pos);
					pos++;
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
				iterator.pos.buffer_pos = pos;
				return RowStep::HAND_OVER;
			}
		}
	}

	//! Walks rows from the skip block: unprojected fields cost nothing, and the value, quote and row callbacks are
	//! the byte loop's own. Hands over to the byte loop, with `states` and the position as it would have them, at
	//! the first byte it does not model. Returns true when the chunk is complete or `to_pos` is reached
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
		const auto &options = state_machine->state_machine_options;
		const char delimiter = options.delimiter.GetValue()[0];
		const char quote = options.quote.GetValue();
		const bool escaped_quotes = options.escape.GetValue() != '\0';
		const bool strict_quotes = options.strict_mode.GetValue();
		const bool carry_on = options.new_line.GetValue() == NewLineIdentifier::CARRY_ON;
		const char *buffer = buffer_handle_ptr;
		idx_t pos = iterator.pos.buffer_pos;
		idx_t run_start = pos;
		while (true) {
			const auto stop = AdvanceToStop(to_pos, pos);
			const CSVState after_run = AfterRun(cur, buffer, run_start, pos);
			if (stop != StopResult::FOUND) {
				states.states[1] = after_run;
				iterator.pos.buffer_pos = pos;
				return stop == StopResult::LIMIT;
			}
			const char byte = buffer[pos];
			RowStep step;
			if (byte == delimiter) {
				states.states[0] = after_run;
				states.states[1] = CSVState::DELIMITER;
				iterator.pos.buffer_pos = pos;
				// the value lies in ASCII-only blocks when it starts inside the current ASCII run
				result.field_is_ascii = skip_block.ascii_start <= result.last_position.buffer_pos;
				T::AddValue(result, pos);
				result.field_is_ascii = false;
				pos++;
				step = RowStep::CONTINUE;
			} else if (byte == '\n' || byte == '\r') {
				if (pos == run_start && (cur == CSVState::RECORD_SEPARATOR || cur == CSVState::NOT_SET)) {
					// an empty row depends on the mode, the byte loop decides
					states.states[1] = after_run;
					iterator.pos.buffer_pos = pos;
					return false;
				}
				step = RowEnd(result, to_pos, carry_on, after_run, pos);
			} else if (byte == quote && after_run != CSVState::STANDARD) {
				// a quote opens a quoted field here, inside a field it is content
				if (!strict_quotes) {
					states.states[1] = after_run;
					iterator.pos.buffer_pos = pos;
					return false;
				}
				states.states[0] = after_run;
				states.states[1] = CSVState::QUOTED;
				ever_quoted = true;
				iterator.pos.buffer_pos = pos;
				T::SetQuoted(result, pos);
				pos++;
				step = QuotedField(result, to_pos, delimiter, quote, escaped_quotes, carry_on, pos);
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
		const bool only_rn_newlines =
		    state_machine->state_machine_options.strict_mode.GetValue() &&
		    state_machine->state_machine_options.strict_mode.IsSetByUser() &&
		    state_machine->state_machine_options.new_line.GetValue() == NewLineIdentifier::CARRY_ON &&
		    state_machine->state_machine_options.new_line.IsSetByUser();
		const idx_t start_pos = iterator.pos.buffer_pos;
		if (iterator.IsBoundarySet()) {
			to_pos = iterator.GetEndPos();
			if (to_pos > cur_buffer_handle->actual_size) {
				to_pos = cur_buffer_handle->actual_size;
			}
		} else {
			to_pos = cur_buffer_handle->actual_size;
		}
		BindSkipBlock();
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
					SkipUntilStop(state_machine->transition_array.skip_quoted, to_pos);
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
					SkipUntilStop(state_machine->transition_array.skip_standard, to_pos);
					break;
				case CSVState::QUOTED_NEW_LINE:
					T::QuotedNewLine(result);
					iterator.pos.buffer_pos++;
					break;
				case CSVState::COMMENT:
					T::SetComment(result, iterator.pos.buffer_pos);
					iterator.pos.buffer_pos++;
					SkipUntilStop(state_machine->transition_array.skip_comment, to_pos);
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
