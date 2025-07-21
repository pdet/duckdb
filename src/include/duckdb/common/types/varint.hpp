//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/varint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/varint.hpp"

namespace duckdb {
using digit_t = uint32_t;
using twodigit_t = uint64_t;

//! The Varint class is a static class that holds helper functions for the Varint type.
class Varint {
public:
	//! Header size of a Varint is always 3 bytes.
	DUCKDB_API static constexpr uint8_t VARINT_HEADER_SIZE = 3;
	//! Max(e such that 10**e fits in a digit_t)
	DUCKDB_API static constexpr uint8_t DECIMAL_SHIFT = 9;
	//! 10 ** DECIMAL_SHIFT
	DUCKDB_API static constexpr digit_t DECIMAL_BASE = 1000000000;
	//! Bytes of a digit_t
	DUCKDB_API static constexpr uint8_t DIGIT_BYTES = sizeof(digit_t);
	//! Bits of a digit_t
	DUCKDB_API static constexpr uint8_t DIGIT_BITS = DIGIT_BYTES * 8;

	template <class T>
	DUCKDB_API static bool TryCast(varint_t input, T &result);

	template <class T>
	static T Cast(varint_t input) {
		T result = 0;
		TryCast(input, result);
		return result;
	}

	template <class T>
	static bool TryConvert(T value, varint_t &result);

	template <class T>
	static varint_t Convert(T value) {
		varint_t result;
		if (!TryConvert(value, result)) { // LCOV_EXCL_START
			throw OutOfRangeException(double(value), GetTypeId<T>(), GetTypeId<varint_t>());
		} // LCOV_EXCL_STOP
		return result;
	}

	//! Verifies if a Varint is valid. i.e., if it has 3 header bytes. The header correctly represents the number of
	//! data bytes, and the data bytes has no leading zero bytes.
	DUCKDB_API static void Verify(const string_t &input);
	//! Reads the size and if the number is negative based on a header
	DUCKDB_API static void ReadHeader(const uint8_t *data, uint8_t &number_of_bytes, bool &is_negative);

	//! Sets the header of a varint (i.e., char* blob), depending on the number of bytes that varint needs and if it's a
	//! negative number
	DUCKDB_API static void SetHeader(char *blob, uint64_t number_of_bytes, bool is_negative);
	//! Initializes and returns a blob with value 0, allocated in Vector& result
	DUCKDB_API static string_t InitializeVarintZero(Vector &result);
	DUCKDB_API static string InitializeVarintZero();

	//! Switch Case of To Varint Convertion
	DUCKDB_API static BoundCastInfo NumericToVarintCastSwitch(const LogicalType &source);

	//! ----------------------------------- Varchar Cast ----------------------------------- //
	//! Function to prepare a varchar for conversion. We trim zero's, check for negative values, and what-not
	//! Returns false if this is an invalid varchar
	DUCKDB_API static bool VarcharFormatting(const string_t &value, idx_t &start_pos, idx_t &end_pos, bool &is_negative,
	                                         bool &is_zero);

	//! Converts a char to a Digit
	DUCKDB_API static int CharToDigit(char c);
	//! Converts a Digit to a char
	DUCKDB_API static char DigitToChar(int digit);
	//! Function to convert a string_t into a vector of bytes
	DUCKDB_API static void GetByteArray(vector<uint8_t> &byte_array, bool &is_negative, const string_t &blob);
	//! Function to create a VARINT blob from a byte array containing the absolute value, plus an is_negative bool
	DUCKDB_API static string FromByteArray(uint8_t *data, idx_t size, bool is_negative);
	//! Function to convert VARINT blob to a VARCHAR
	DUCKDB_API static string VarIntToVarchar(const string_t &blob);
	//! Function to convert Varchar to VARINT blob
	DUCKDB_API static string VarcharToVarInt(const string_t &value);
	//! ----------------------------------- Double Cast ----------------------------------- //
	DUCKDB_API static bool VarintToDouble(const string_t &blob, double &result, bool &strict);
};

template <>
DUCKDB_API bool Varint::TryCast(varint_t input, bool &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, int8_t &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, int16_t &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, int32_t &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, int64_t &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, uint8_t &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, uint16_t &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, uint32_t &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, uint64_t &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, hugeint_t &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, uhugeint_t &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, float &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, double &result);
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, long double &result);

template <>
bool Varint::TryConvert(int8_t value, varint_t &result);
template <>
bool Varint::TryConvert(int16_t value, varint_t &result);
template <>
bool Varint::TryConvert(int32_t value, varint_t &result);
template <>
bool Varint::TryConvert(int64_t value, varint_t &result);
template <>
bool Varint::TryConvert(uint8_t value, varint_t &result);
template <>
bool Varint::TryConvert(uint16_t value, varint_t &result);
template <>
bool Varint::TryConvert(uint32_t value, varint_t &result);
template <>
bool Varint::TryConvert(uint64_t value, varint_t &result);
template <>
bool Varint::TryConvert(float value, varint_t &result);
template <>
bool Varint::TryConvert(double value, varint_t &result);
template <>
bool Varint::TryConvert(long double value, varint_t &result);
template <>
bool Varint::TryConvert(const char *value, varint_t &result);

//! ----------------------------------- (u)Integral Cast ----------------------------------- //
struct IntCastToVarInt {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		return IntToVarInt(result, input);
	}
};

//! ----------------------------------- (u)HugeInt Cast ----------------------------------- //
struct HugeintCastToVarInt {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw InternalException("Unsupported type for cast to VARINT");
	}
};

struct TryCastToVarInt {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, Vector &result_vector, CastParameters &parameters) {
		throw InternalException("Unsupported type for try cast to VARINT");
	}
};

template <>
DUCKDB_API bool TryCastToVarInt::Operation(double double_value, string_t &result_value, Vector &result,
                                           CastParameters &parameters);

template <>
DUCKDB_API bool TryCastToVarInt::Operation(float float_value, string_t &result_value, Vector &result,
                                           CastParameters &parameters);

template <>
DUCKDB_API bool TryCastToVarInt::Operation(string_t input_value, string_t &result_value, Vector &result,
                                           CastParameters &parameters);

struct VarIntCastToVarchar {
	template <class SRC>
	DUCKDB_API static inline string_t Operation(SRC input, Vector &result) {
		return StringVector::AddStringOrBlob(result, Varint::VarIntToVarchar(input));
	}
};

struct VarintToDoubleCast {
	template <class SRC, class DST>
	DUCKDB_API static inline bool Operation(SRC input, DST &result, bool strict = false) {
		return Varint::VarintToDouble(input, result, strict);
	}
};

} // namespace duckdb
