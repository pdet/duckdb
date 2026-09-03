//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/swar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/bswap.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! Word-at-a-time (SWAR) primitives over the eight bytes of a uint64_t
//! Masks returned here flag a byte by setting its high bit
struct SwarWord {
	static constexpr idx_t SIZE = sizeof(uint64_t);
	static constexpr uint64_t LSB = 0x0101010101010101ULL;
	static constexpr uint64_t MSB = 0x8080808080808080ULL;
	static constexpr uint64_t LOW7 = 0x7F7F7F7F7F7F7F7FULL;

	//! A word with every byte set to `byte`
	static inline uint64_t Repeat(uint8_t byte) {
		return LSB * byte;
	}

	//! Flags every byte of `word` that is zero, exact for all byte values
	static inline uint64_t ZeroBytes(uint64_t word) {
		return ~(((word & LOW7) + LOW7) | word) & MSB;
	}

	//! Flags every byte of `word` that is equal to `byte`
	static inline uint64_t EqualBytes(uint64_t word, uint8_t byte) {
		return ZeroBytes(word ^ Repeat(byte));
	}

	//! Whether no byte of `word` has its high bit set
	static inline bool IsAscii(uint64_t word) {
		return (word & MSB) == 0;
	}

	//! Sums up the individual bytes of `word` - only valid if the sum does not exceed 255
	static inline idx_t SumBytes(uint64_t word) {
		return static_cast<idx_t>((word * LSB) >> 56);
	}

	//! The number of flagged bytes in a mask
	static inline idx_t CountFlagged(uint64_t mask) {
		// every flagged byte has its high bit set - shift it down so each byte is a zero or a one
		return SumBytes(mask >> 7);
	}

	//! The index (in memory order) of the first flagged byte in a mask
	static inline idx_t FirstFlagged(uint64_t mask) {
#if DUCKDB_IS_BIG_ENDIAN
		return CountZeros<uint64_t>::Leading(mask) / 8;
#else
		return CountZeros<uint64_t>::Trailing(mask) / 8;
#endif
	}

	//! Packs the flags of a mask into the low eight bits, byte i (in memory order) to bit i
	static inline uint64_t PackFlags(uint64_t mask) {
#if DUCKDB_IS_BIG_ENDIAN
		mask = BSwap(mask);
#endif
		return ((mask >> 7) * PACK_MULTIPLIER) >> 56;
	}

private:
	//! Moves the flag of byte i to bit 56 + i
	static constexpr uint64_t PACK_MULTIPLIER = 0x0102040810204080ULL;
};

//! Bit masks over 64-byte blocks, one bit per byte (bit i is byte i)
struct SwarBlock {
	static constexpr idx_t SIZE = 64;
	static constexpr idx_t WORDS = SIZE / SwarWord::SIZE;

	//! Mask of the bytes in the block that are equal to `byte`
	static inline uint64_t EqualMask(const char *block, char byte) {
		const uint64_t pattern = SwarWord::Repeat(static_cast<uint8_t>(byte));
		uint64_t mask = 0;
		for (idx_t i = 0; i < WORDS; i++) {
			const auto word = Load<uint64_t>(const_data_ptr_cast(block + i * SwarWord::SIZE));
			mask |= SwarWord::PackFlags(SwarWord::ZeroBytes(word ^ pattern)) << (i * SwarWord::SIZE);
		}
		return mask;
	}

	//! Whether every byte in the block is ASCII
	static inline bool IsAscii(const char *block) {
		uint64_t any = 0;
		for (idx_t i = 0; i < WORDS; i++) {
			any |= Load<uint64_t>(const_data_ptr_cast(block + i * SwarWord::SIZE));
		}
		return SwarWord::IsAscii(any);
	}

	//! Inclusive prefix XOR over a mask: bit i of the result is the XOR of bits 0 through i
	static inline uint64_t PrefixXor(uint64_t mask) {
		mask ^= mask << 1;
		mask ^= mask << 2;
		mask ^= mask << 4;
		mask ^= mask << 8;
		mask ^= mask << 16;
		mask ^= mask << 32;
		return mask;
	}
};

} // namespace duckdb
