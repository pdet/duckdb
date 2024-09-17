#include "duckdb/execution/operator/csv_scanner/base_scanner.hpp"

#include "duckdb/execution/operator/csv_scanner/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/skip_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/string_value_scanner.hpp"

namespace duckdb {

ScannerResult::ScannerResult(CSVStates &states_p, CSVStateMachine &state_machine_p, idx_t result_size_p)
    : result_size(result_size_p), state_machine(state_machine_p), states(states_p) {
}

BaseScanner::BaseScanner(shared_ptr<CSVBufferManager> buffer_manager_p, shared_ptr<CSVStateMachine> state_machine_p,
                         shared_ptr<CSVErrorHandler> error_handler_p, bool sniffing_p,
                         shared_ptr<CSVFileScan> csv_file_scan_p, CSVIterator iterator_p)
    : csv_file_scan(std::move(csv_file_scan_p)), sniffing(sniffing_p), error_handler(std::move(error_handler_p)),
      state_machine(std::move(state_machine_p)), buffer_manager(std::move(buffer_manager_p)), iterator(iterator_p) {
	D_ASSERT(buffer_manager);
	D_ASSERT(state_machine);
	// Initialize current buffer handle
	cur_buffer_handle = buffer_manager->GetBuffer(iterator.GetBufferIdx());
	if (!cur_buffer_handle) {
		buffer_handle_ptr = nullptr;
	} else {
		buffer_handle_ptr = cur_buffer_handle->Ptr();
	}
}

bool BaseScanner::FinishedFile() {
	if (!cur_buffer_handle) {
		return true;
	}
	// we have to scan to infinity, so we must check if we are done checking the whole file
	if (!buffer_manager->Done()) {
		return false;
	}
	// If yes, are we in the last buffer?
	if (iterator.pos.buffer_idx != buffer_manager->BufferCount()) {
		return false;
	}
	// If yes, are we in the last position?
	return iterator.pos.buffer_pos + 1 == cur_buffer_handle->actual_size;
}

CSVIterator BaseScanner::SkipCSVRows(shared_ptr<CSVBufferManager> buffer_manager,
                                     const shared_ptr<CSVStateMachine> &state_machine, idx_t rows_to_skip) {
	if (rows_to_skip == 0) {
		return {};
	}
	auto error_handler = make_shared_ptr<CSVErrorHandler>();
	SkipScanner row_skipper(std::move(buffer_manager), state_machine, error_handler, rows_to_skip);
	row_skipper.ParseChunk();
	return row_skipper.GetIterator();
}

CSVIterator &BaseScanner::GetIterator() {
	return iterator;
}

void BaseScanner::SetIterator(const CSVIterator &it) {
	iterator = it;
}

ScannerResult &BaseScanner::ParseChunk() {
	throw InternalException("ParseChunk() from CSV Base Scanner is not implemented");
}

ScannerResult &BaseScanner::GetResult() {
	throw InternalException("GetResult() from CSV Base Scanner is not implemented");
}

void BaseScanner::Initialize() {
	throw InternalException("Initialize() from CSV Base Scanner is not implemented");
}

void BaseScanner::FinalizeChunkProcess() {
	throw InternalException("FinalizeChunkProcess() from CSV Base Scanner is not implemented");
}

void BaseScanner::SkipUntilNewLine() {
	// Now skip until next newline
	if (state_machine->options.dialect_options.state_machine_options.new_line.GetValue() ==
	    NewLineIdentifier::CARRY_ON) {
		bool carriage_return = false;
		bool not_carriage_return = false;
		for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\r') {
				carriage_return = true;
			} else if (buffer_handle_ptr[iterator.pos.buffer_pos] != '\n') {
				not_carriage_return = true;
			}
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\n') {
				if (carriage_return || not_carriage_return) {
					iterator.pos.buffer_pos++;
					return;
				}
			}
		}
	} else {
		for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\n' ||
			    buffer_handle_ptr[iterator.pos.buffer_pos] == '\r') {
				iterator.pos.buffer_pos++;
				return;
			}
		}
	}
}

void BaseScanner::FindNewLine(ScannerResult &result) {
	// The result size of the data after skipping the row is one line
	// We have to look for a new line that fits our schema
	// 1. We walk until the next new line
	bool line_found;
	unique_ptr<StringValueScanner> scan_finder;
	do {
		constexpr idx_t result_size = 1;
		SkipUntilNewLine();
		if (state_machine->options.null_padding) {
			// When Null Padding, we assume we start from the correct new-line
			return;
		}
		scan_finder =
		    make_uniq<StringValueScanner>(0U, buffer_manager, state_machine, make_shared_ptr<CSVErrorHandler>(true),
		                                  csv_file_scan, false, iterator, result_size);
		auto &tuples = scan_finder->ParseChunk();
		line_found = true;
		if (tuples.number_of_rows != 1 ||
		    (!tuples.borked_rows.empty() && !state_machine->options.ignore_errors.GetValue()) ||
		    tuples.first_line_is_comment) {
			line_found = false;
			// If no tuples were parsed, this is not the correct start, we need to skip until the next new line
			// Or if columns don't match, this is not the correct start, we need to skip until the next new line
			if (scan_finder->previous_buffer_handle) {
				if (scan_finder->iterator.pos.buffer_pos >= scan_finder->previous_buffer_handle->actual_size &&
				    scan_finder->previous_buffer_handle->is_last_buffer) {
					iterator.pos.buffer_idx = scan_finder->iterator.pos.buffer_idx;
					iterator.pos.buffer_pos = scan_finder->iterator.pos.buffer_pos;
					result.last_position = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, result.buffer_size};
					iterator.done = scan_finder->iterator.done;
					return;
				}
			}
			if (iterator.pos.buffer_pos == cur_buffer_handle->actual_size ||
			    scan_finder->iterator.GetBufferIdx() > iterator.GetBufferIdx()) {
				// If things go terribly wrong, we never loop indefinitely.
				iterator.pos.buffer_idx = scan_finder->iterator.pos.buffer_idx;
				iterator.pos.buffer_pos = scan_finder->iterator.pos.buffer_pos;
				result.last_position = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, result.buffer_size};
				iterator.done = scan_finder->iterator.done;
				return;
			}
		}
	} while (!line_found);
	iterator.pos.buffer_idx = scan_finder->result.current_line_position.begin.buffer_idx;
	iterator.pos.buffer_pos = scan_finder->result.current_line_position.begin.buffer_pos;
	result.last_position = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, result.buffer_size};
}

CSVStateMachine &BaseScanner::GetStateMachine() const {
	return *state_machine;
}

} // namespace duckdb
