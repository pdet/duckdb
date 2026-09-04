//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_structural_cursor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/swar.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Finds the structural bytes of a buffer through a stop mask cached for one block of 64 bytes at a time
class CSVStructuralCursor {
public:
	enum class Stop : uint8_t { FOUND, LIMIT, TAIL };

	//! Adds a byte pattern that ends a skip, bytes match it on the bits set in `mask`
	void AddPattern(uint8_t value, uint8_t mask) {
		patterns.emplace_back(value, mask);
	}

	//! Binds the cursor to a buffer, identified by index and address, dropping the block of a previous one
	void Bind(idx_t buffer_idx_p, const char *buffer_p, idx_t buffer_size_p) {
		if (buffer_idx == buffer_idx_p && buffer == buffer_p) {
			return;
		}
		buffer_idx = buffer_idx_p;
		buffer = buffer_p;
		buffer_size = buffer_size_p;
		start = 0;
		end = 0;
		ascii_start = DConstants::INVALID_INDEX;
	}

	//! Moves `pos` over unflagged bytes to a flagged one (FOUND), to `limit` (LIMIT) or into the buffer tail (TAIL)
	inline Stop AdvanceToStop(const idx_t limit, idx_t &pos) {
		while (pos < limit) {
			if (pos < start || pos >= end) {
				if (pos + SwarBlock::SIZE > buffer_size) {
					return Stop::TAIL;
				}
				LoadBlock(pos);
			}
			const uint64_t remaining = stops >> (pos - start);
			if (remaining) {
				const idx_t stop = pos + CountZeros<uint64_t>::Trailing(remaining);
				if (stop < limit) {
					pos = stop;
					return Stop::FOUND;
				}
			}
			pos = MinValue<idx_t>(remaining ? limit : end, limit);
		}
		return Stop::LIMIT;
	}

	//! Moves `pos` to the next byte `skip_table` does not skip, or to the byte before `to_pos`, through the block
	inline void SkipUntilStop(const bool *skip_table, const idx_t to_pos, idx_t &pos) {
		while (pos + 1 < to_pos) {
			const auto stop = AdvanceToStop(to_pos - 1, pos);
			if (stop == Stop::TAIL) {
				// the tail of the buffer is walked byte by byte
				while (skip_table[static_cast<uint8_t>(buffer[pos])] && pos + 1 < to_pos) {
					pos++;
				}
				return;
			}
			if (stop == Stop::LIMIT || !skip_table[static_cast<uint8_t>(buffer[pos])]) {
				return;
			}
			// a flagged byte the skip table skips after all
			pos++;
		}
	}

	//! Whether the bytes from `from` to the end of the current block lie in ASCII only blocks
	bool AsciiFrom(const idx_t from) const {
		return ascii_start <= from;
	}

private:
	//! Computes the stop mask of the block that starts at `block_start`
	void LoadBlock(const idx_t block_start) {
		const char *block = buffer + block_start;
		stops = SwarBlock::MaybeAnyMask(block, patterns.data(), patterns.size());
		if (!SwarBlock::IsAscii(block)) {
			ascii_start = DConstants::INVALID_INDEX;
		} else if (ascii_start == DConstants::INVALID_INDEX || end != block_start) {
			ascii_start = block_start;
		}
		start = block_start;
		end = block_start + SwarBlock::SIZE;
	}

	//! The byte patterns that end a skip
	vector<SwarBlock::BytePattern> patterns;
	//! The bound buffer, by index and by address, and its size
	idx_t buffer_idx = DConstants::INVALID_INDEX;
	const char *buffer = nullptr;
	idx_t buffer_size = 0;
	//! The loaded block [start, end) and its stop mask, bit i is byte start + i
	idx_t start = 0;
	idx_t end = 0;
	uint64_t stops = 0;
	//! The start of the run of ASCII only blocks that ends with the loaded block, invalid if it is not ASCII
	idx_t ascii_start = DConstants::INVALID_INDEX;
};

} // namespace duckdb
