#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/base_scanner.hpp"

namespace duckdb {

ScannerResult::ScannerResult(CSVStates &states_p, CSVStateMachine &state_machine_p)
    : states(states_p), state_machine(state_machine_p) {
}

idx_t ScannerResult::Size() {
	return result_position;
}

bool ScannerResult::Empty() {
	return result_position == 0;
}

BaseScanner::BaseScanner(shared_ptr<CSVBufferManager> buffer_manager_p, shared_ptr<CSVStateMachine> state_machine_p,
                         shared_ptr<CSVErrorHandler> error_handler_p, CSVIterator iterator_p)
    : error_handler(std::move(error_handler_p)), state_machine(std::move(state_machine_p)), iterator(iterator_p),
      buffer_manager(std::move(buffer_manager_p)) {
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

void BaseScanner::Reset() {
	iterator.SetCurrentPositionToBoundary();
	lines_read = 0;
}

CSVIterator &BaseScanner::GetIterator() {
	return iterator;
}

ScannerResult &BaseScanner::ParseChunk() {
	throw InternalException("ParseChunk() from CSV Base Scanner is mot implemented");
}

ScannerResult &BaseScanner::GetResult() {
	throw InternalException("GetResult() from CSV Base Scanner is mot implemented");
}

void BaseScanner::Initialize() {
	throw InternalException("Initialize() from CSV Base Scanner is mot implemented");
}

template <class T>
void BaseScanner::Process(T &result) {
	idx_t to_pos;
	if (iterator.IsBoundarySet()) {
		to_pos = iterator.GetEndPos();
		if (to_pos > cur_buffer_handle->actual_size) {
			to_pos = cur_buffer_handle->actual_size;
		}
	} else {
		to_pos = cur_buffer_handle->actual_size;
	}
	// operate over the buffer
	idx_t state_end = iterator.states_end;
	if (iterator.continue_from_states_pos){
		states.cur_pos = iterator.states_pos;
	}
	iterator.continue_from_states_pos = false;
	while (iterator.pos.buffer_pos < to_pos){
		uint8_t cur_state_pos = states.cur_pos + 1;
		// We first fill our states buffer with 255 pos or the total buffer size
		if (!iterator.continue_from_states_pos){
			state_end = to_pos - iterator.pos.buffer_pos < 255 ? to_pos - iterator.pos.buffer_pos : 255;
			for (idx_t i = 0; i < state_end; i++){
				state_machine->Transition(states, buffer_handle_ptr[iterator.pos.buffer_pos+i]);
			}
		}
		idx_t skip_end = (state_end -4)/4;
		if (state_end < 4){
			skip_end=0;
		}
		for (idx_t  i = 0; i < skip_end; i++){
			idx_t actual_idx = i*4;
			if (static_cast<uint8_t>(cur_state_pos + actual_idx) < 252){
				if (*reinterpret_cast<int32_t*>(&states.states[static_cast<uint8_t>(cur_state_pos + actual_idx)]) == 0){
					continue;
				}
			}
			for (idx_t j = 0; j < 4; j++){
				if (ProcessCharacter(*this,cur_state_pos+actual_idx+j, iterator.pos.buffer_pos + actual_idx+j, result)) {
					iterator.pos.buffer_pos += actual_idx+j + 1;
					iterator.continue_from_states_pos = true;
					iterator.states_pos = cur_state_pos + actual_idx+j;
					iterator.states_end = state_end - (actual_idx+j+1);
					return;
			}
			}
		}
		for (idx_t i = skip_end*4; i < state_end; i++){
			// Do some SIMD-like skipping
			if (static_cast<uint8_t> (cur_state_pos + i) < 252 && i + 4 < state_end ){
				if (*reinterpret_cast<int32_t*>(&states.states[static_cast<uint8_t>(cur_state_pos + i)]) == 0){
					i+=3;
					continue;
				}
			}
			if (ProcessCharacter(*this,cur_state_pos+i, iterator.pos.buffer_pos + i, result)) {
				iterator.pos.buffer_pos += i + 1;
				iterator.continue_from_states_pos = true;
				iterator.states_pos = cur_state_pos + i;
				iterator.states_end = state_end - (i+1);
				return;
			}
		}
		// Now we actually turn the buffer into strings
//		for (idx_t i = 0; i < state_end; i++){
//			// Do some SIMD-like skipping
//			if (static_cast<uint8_t> (cur_state_pos + i) < 252 && i + 4 < state_end ){
//				if (*reinterpret_cast<int32_t*>(&states.states[static_cast<uint8_t>(cur_state_pos + i)]) == 0){
//					i+=3;
//					continue;
//				}
//			}
//			if (ProcessCharacter(*this,cur_state_pos+i, iterator.pos.buffer_pos + i, result)) {
//				iterator.pos.buffer_pos += i + 1;
//				iterator.continue_from_states_pos = true;
//				iterator.states_pos = cur_state_pos + i;
//				iterator.states_end = state_end - (i+1);
//				return;
//			}
//		}
		iterator.pos.buffer_pos +=state_end;
	}
}

void BaseScanner::FinalizeChunkProcess() {
	throw InternalException("FinalizeChunkProcess() from CSV Base Scanner is mot implemented");
}

template <class T>
void BaseScanner::ParseChunkInternal(T &result) {
	if (!initialized) {
		Initialize();
		initialized = true;
	}
	Process(result);
	FinalizeChunkProcess();
}

CSVStateMachine &BaseScanner::GetStateMachine() {
	return *state_machine;
}

} // namespace duckdb
