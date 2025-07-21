#include "duckdb/common/varint.hpp"
#include "duckdb/common/types/varint.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/typedefs.hpp"
#include <cmath>

namespace duckdb {

void Varint::Verify(const string_t &input) {
#ifdef DEBUG
	// Size must be >= 4
	idx_t varint_bytes = input.GetSize();
	if (varint_bytes < 4) {
		throw InternalException("Varint number of bytes is invalid, current number of bytes is %d", varint_bytes);
	}
	// Bytes in header must quantify the number of data bytes
	auto varint_ptr = input.GetData();
	bool is_negative = (varint_ptr[0] & 0x80) == 0;
	uint32_t number_of_bytes = 0;
	char mask = 0x7F;
	if (is_negative) {
		number_of_bytes |= static_cast<uint32_t>(~varint_ptr[0] & mask) << 16 & 0xFF0000;
		number_of_bytes |= static_cast<uint32_t>(~varint_ptr[1]) << 8 & 0xFF00;
		;
		number_of_bytes |= static_cast<uint32_t>(~varint_ptr[2]) & 0xFF;
	} else {
		number_of_bytes |= static_cast<uint32_t>(varint_ptr[0] & mask) << 16 & 0xFF0000;
		number_of_bytes |= static_cast<uint32_t>(varint_ptr[1]) << 8 & 0xFF00;
		number_of_bytes |= static_cast<uint32_t>(varint_ptr[2]) & 0xFF;
	}
	if (number_of_bytes != varint_bytes - 3) {
		throw InternalException("The number of bytes set in the Varint header: %d bytes. Does not "
		                        "match the number of bytes encountered as the varint data: %d bytes.",
		                        number_of_bytes, varint_bytes - 3);
	}
	//  No bytes between 4 and end can be 0, unless total size == 4
	if (varint_bytes > 4) {
		if (is_negative) {
			if (static_cast<data_t>(~varint_ptr[3]) == 0) {
				throw InternalException("Invalid top data bytes set to 0 for VARINT values");
			}
		} else {
			if (varint_ptr[3] == 0) {
				throw InternalException("Invalid top data bytes set to 0 for VARINT values");
			}
		}
	}
#endif
}
void Varint::SetHeader(char *blob, uint64_t number_of_bytes, bool is_negative) {
	uint32_t header = static_cast<uint32_t>(number_of_bytes);
	// Set MSBit of 3rd byte
	header |= 0x00800000;
	if (is_negative) {
		header = ~header;
	}
	// we ignore MSByte  of header.
	// write the 3 bytes to blob.
	blob[0] = static_cast<char>(header >> 16);
	blob[1] = static_cast<char>(header >> 8 & 0xFF);
	blob[2] = static_cast<char>(header & 0xFF);
}

// Creates a blob representing the value 0
string_t Varint::InitializeVarintZero(Vector &result) {
	uint32_t blob_size = 1 + VARINT_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = blob.GetDataWriteable();
	SetHeader(writable_blob, 1, false);
	writable_blob[3] = 0;
	blob.Finalize();
	return blob;
}

string Varint::InitializeVarintZero() {
	uint32_t blob_size = 1 + VARINT_HEADER_SIZE;
	string result(blob_size, '0');
	SetHeader(&result[0], 1, false);
	result[3] = 0;
	return result;
}

int Varint::CharToDigit(char c) {
	return c - '0';
}

char Varint::DigitToChar(int digit) {
	// FIXME: this would be the proper solution:
	// return UnsafeNumericCast<char>(digit + '0');
	return static_cast<char>(digit + '0');
}

bool Varint::VarcharFormatting(const string_t &value, idx_t &start_pos, idx_t &end_pos, bool &is_negative,
                               bool &is_zero) {
	// If it's empty we error
	if (value.Empty()) {
		return false;
	}
	start_pos = 0;
	is_zero = false;

	auto int_value_char = value.GetData();
	end_pos = value.GetSize();

	// If first character is -, we have a negative number, if + we have a + number
	is_negative = int_value_char[0] == '-';
	if (is_negative) {
		start_pos++;
	}
	if (int_value_char[0] == '+') {
		start_pos++;
	}
	// Now lets trim 0s
	bool at_least_one_zero = false;
	while (start_pos < end_pos && int_value_char[start_pos] == '0') {
		start_pos++;
		at_least_one_zero = true;
	}
	if (start_pos == end_pos) {
		if (at_least_one_zero) {
			// This is a 0 value
			is_zero = true;
			return true;
		}
		// This is either a '+' or '-'. Hence, invalid.
		return false;
	}
	idx_t cur_pos = start_pos;
	// Verify all is numeric
	while (cur_pos < end_pos && std::isdigit(int_value_char[cur_pos])) {
		cur_pos++;
	}
	if (cur_pos < end_pos) {
		idx_t possible_end = cur_pos;
		// Oh oh, this is not a digit, if it's a . we might be fine, otherwise, this is invalid.
		if (int_value_char[cur_pos] == '.') {
			cur_pos++;
		} else {
			return false;
		}

		while (cur_pos < end_pos) {
			if (std::isdigit(int_value_char[cur_pos])) {
				cur_pos++;
			} else {
				// By now we can only have numbers, otherwise this is invalid.
				return false;
			}
		}
		// Floor cast this boy
		end_pos = possible_end;
	}
	return true;
}

void Varint::GetByteArray(vector<uint8_t> &byte_array, bool &is_negative, const string_t &blob) {
	if (blob.GetSize() < 4) {
		throw InvalidInputException("Invalid blob size.");
	}
	auto blob_ptr = blob.GetData();

	// Determine if the number is negative
	is_negative = (blob_ptr[0] & 0x80) == 0;
	byte_array.reserve(blob.GetSize() - 3);
	if (is_negative) {
		for (idx_t i = 3; i < blob.GetSize(); i++) {
			byte_array.push_back(static_cast<uint8_t>(~blob_ptr[i]));
		}
	} else {
		for (idx_t i = 3; i < blob.GetSize(); i++) {
			byte_array.push_back(static_cast<uint8_t>(blob_ptr[i]));
		}
	}
}

string Varint::FromByteArray(uint8_t *data, idx_t size, bool is_negative) {
	string result(VARINT_HEADER_SIZE + size, '0');
	SetHeader(&result[0], size, is_negative);
	uint8_t *result_data = reinterpret_cast<uint8_t *>(&result[VARINT_HEADER_SIZE]);
	if (is_negative) {
		for (idx_t i = 0; i < size; i++) {
			result_data[i] = ~data[i];
		}
	} else {
		for (idx_t i = 0; i < size; i++) {
			result_data[i] = data[i];
		}
	}
	return result;
}

// Following CPython and Knuth (TAOCP, Volume 2 (3rd edn), section 4.4, Method 1b).
string Varint::VarIntToVarchar(const string_t &blob) {
	string decimal_string;
	vector<uint8_t> byte_array;
	bool is_negative;
	GetByteArray(byte_array, is_negative, blob);
	vector<digit_t> digits;
	// Rounding byte_array to digit_bytes multiple size, so that we can process every digit_bytes bytes
	// at a time without if check in the for loop
	idx_t padding_size = (-byte_array.size()) & (DIGIT_BYTES - 1);
	byte_array.insert(byte_array.begin(), padding_size, 0);
	for (idx_t i = 0; i < byte_array.size(); i += DIGIT_BYTES) {
		digit_t hi = 0;
		for (idx_t j = 0; j < DIGIT_BYTES; j++) {
			hi |= UnsafeNumericCast<digit_t>(byte_array[i + j]) << (8 * (DIGIT_BYTES - j - 1));
		}

		for (idx_t j = 0; j < digits.size(); j++) {
			twodigit_t tmp = UnsafeNumericCast<twodigit_t>(digits[j]) << DIGIT_BITS | hi;
			hi = static_cast<digit_t>(tmp / UnsafeNumericCast<twodigit_t>(DECIMAL_BASE));
			digits[j] = static_cast<digit_t>(tmp - UnsafeNumericCast<twodigit_t>(DECIMAL_BASE * hi));
		}

		while (hi) {
			digits.push_back(hi % DECIMAL_BASE);
			hi /= DECIMAL_BASE;
		}
	}

	if (digits.empty()) {
		digits.push_back(0);
	}

	for (idx_t i = 0; i < digits.size() - 1; i++) {
		auto remain = digits[i];
		for (idx_t j = 0; j < DECIMAL_SHIFT; j++) {
			decimal_string += DigitToChar(static_cast<int>(remain % 10));
			remain /= 10;
		}
	}

	auto remain = digits.back();
	do {
		decimal_string += DigitToChar(static_cast<int>(remain % 10));
		remain /= 10;
	} while (remain != 0);

	if (is_negative) {
		decimal_string += '-';
	}
	// Reverse the string to get the correct decimal representation
	std::reverse(decimal_string.begin(), decimal_string.end());
	return decimal_string;
}

string Varint::VarcharToVarInt(const string_t &value) {
	idx_t start_pos, end_pos;
	bool is_negative, is_zero;
	if (!VarcharFormatting(value, start_pos, end_pos, is_negative, is_zero)) {
		throw ConversionException("Could not convert string \'%s\' to Varint", value.GetString());
	}
	if (is_zero) {
		// Return Value 0
		return InitializeVarintZero();
	}
	auto int_value_char = value.GetData();
	idx_t actual_size = end_pos - start_pos;

	// we initalize result with space for our header
	string result(VARINT_HEADER_SIZE, '0');
	unsafe_vector<uint64_t> digits;

	// The max number a uint64_t can represent is 18.446.744.073.709.551.615
	// That has 20 digits
	// In the worst case a remainder of a division will be 255, which is 3 digits
	// Since the max value is 184, we need to take one more digit out
	// Hence we end up with a max of 16 digits supported.
	constexpr uint8_t max_digits = 16;
	const idx_t number_of_digits = static_cast<idx_t>(std::ceil(static_cast<double>(actual_size) / max_digits));

	// lets convert the string to a uint64_t vector
	idx_t cur_end = end_pos;
	for (idx_t i = 0; i < number_of_digits; i++) {
		idx_t cur_start = static_cast<int64_t>(start_pos) > static_cast<int64_t>(cur_end - max_digits)
		                      ? start_pos
		                      : cur_end - max_digits;
		std::string current_number(int_value_char + cur_start, cur_end - cur_start);
		digits.push_back(std::stoull(current_number));
		// move cur_end to more digits down the road
		cur_end = cur_end - max_digits;
	}

	// Now that we have our uint64_t vector, lets start our division process to figure out the new number and remainder
	while (!digits.empty()) {
		idx_t digit_idx = digits.size() - 1;
		uint8_t remainder = 0;
		idx_t digits_size = digits.size();
		for (idx_t i = 0; i < digits_size; i++) {
			digits[digit_idx] += static_cast<uint64_t>(remainder * pow(10, max_digits));
			remainder = static_cast<uint8_t>(digits[digit_idx] % 256);
			digits[digit_idx] /= 256;
			if (digits[digit_idx] == 0 && digit_idx == digits.size() - 1) {
				// we can cap this
				digits.pop_back();
			}
			digit_idx--;
		}
		if (is_negative) {
			result.push_back(static_cast<char>(~remainder));
		} else {
			result.push_back(static_cast<char>(remainder));
		}
	}
	std::reverse(result.begin() + VARINT_HEADER_SIZE, result.end());
	// Set header after we know the size of the varint
	SetHeader(&result[0], result.size() - VARINT_HEADER_SIZE, is_negative);
	return result;
}

bool Varint::VarintToDouble(const string_t &blob, double &result, bool &strict) {
	result = 0;

	if (blob.GetSize() < 4) {
		throw InvalidInputException("Invalid blob size.");
	}
	auto blob_ptr = blob.GetData();

	// Determine if the number is negative
	bool is_negative = (blob_ptr[0] & 0x80) == 0;
	idx_t byte_pos = 0;
	for (idx_t i = blob.GetSize() - 1; i > 2; i--) {
		if (is_negative) {
			result += static_cast<uint8_t>(~blob_ptr[i]) * pow(256, static_cast<double>(byte_pos));
		} else {
			result += static_cast<uint8_t>(blob_ptr[i]) * pow(256, static_cast<double>(byte_pos));
		}
		byte_pos++;
	}

	if (is_negative) {
		result *= -1;
	}
	if (!std::isfinite(result)) {
		// We throw an error
		throw ConversionException("Could not convert varint '%s' to Double", VarIntToVarchar(blob));
	}
	return true;
}

//===--------------------------------------------------------------------===//
// varint_t operators
//===--------------------------------------------------------------------===//

string varint_t::ToString() const {
	return Varint::VarIntToVarchar(value);
}

bool varint_t::operator==(const varint_t &rhs) const {
	return value == rhs.value;
}
bool varint_t::operator!=(const varint_t &rhs) const {
	return value != rhs.value;
}

bool varint_t::operator<(const varint_t &rhs) const {
	return value < rhs.value;
}

bool varint_t::operator<=(const varint_t &rhs) const {
	return value <= rhs.value;
}

bool varint_t::operator>(const varint_t &rhs) const {
	return value > rhs.value;
}
bool varint_t::operator>=(const varint_t &rhs) const {
	return value >= rhs.value;
}

void Varint::ReadHeader(const uint8_t *data, uint8_t &number_of_bytes, bool &is_negative) {

	uint32_t header = (static_cast<uint32_t>(data[0]) << 16) | (static_cast<uint32_t>(data[1]) << 8) |
	                  (static_cast<uint32_t>(data[2]));

	is_negative = (header & 0x00800000) == 0;
	if (is_negative) {
		header = ~header;
	}
	header &= 0x007FFFFF;
	number_of_bytes = static_cast<uint8_t>(header);
}

template <class T>
T VarIntToInt(const varint_t &varint) {
	const auto *data = reinterpret_cast<const uint8_t *>(varint.value[0]);
	uint8_t data_byte_size;
	bool is_negative;

	// Read the header to get sign and size info
	Varint::ReadHeader(data, data_byte_size, is_negative);

	// Sanity check
	if (data_byte_size + Varint::VARINT_HEADER_SIZE > varint.value.size()) {
		throw InvalidInputException("Invalid varint: inconsistent size");
	}

	uint64_t abs_value = 0;
	for (idx_t i = 0; i < data_byte_size; ++i) {
		uint8_t byte = data[Varint::VARINT_HEADER_SIZE + i];
		if (is_negative) {
			byte = ~byte;
		}
		abs_value = (abs_value << 8) | byte;
	}

	if (is_negative) {
		if (abs_value > static_cast<uint64_t>(std::numeric_limits<T>::max()) + 1) {
			throw OutOfRangeException("Negative varint too small for type");
		}
		return static_cast<T>(-static_cast<int64_t>(abs_value));
	} else {
		if (abs_value > static_cast<uint64_t>(std::numeric_limits<T>::max())) {
			throw OutOfRangeException("Positive varint too large for type");
		}
		return static_cast<T>(abs_value);
	}
}
varint_t::operator uint8_t() const {
	return VarIntToInt<uint8_t>(*this);
}
varint_t::operator uint16_t() const {
	return VarIntToInt<uint16_t>(*this);
}
varint_t::operator uint32_t() const {
	return VarIntToInt<uint32_t>(*this);
}
varint_t::operator uint64_t() const {
	return VarIntToInt<uint64_t>(*this);
}
varint_t::operator int8_t() const {
	return VarIntToInt<int8_t>(*this);
}
varint_t::operator int16_t() const {
	return VarIntToInt<int16_t>(*this);
}
varint_t::operator int32_t() const {
	return VarIntToInt<int32_t>(*this);
}
varint_t::operator int64_t() const {
	return VarIntToInt<int64_t>(*this);
}

template <class T>
bool VarintToInteger(varint_t &input, T &result) {
	if (input < NumericLimits<T>::Minimum() || input > NumericLimits<T>::Maximum()) {
		return false;
	}
	// Cast it
	result = Varint::Cast<T>(input);
	return true;
}

template <>
DUCKDB_API bool Varint::TryCast(varint_t input, bool &result) {
	return VarintToInteger(input, result);
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, int8_t &result) {
	return VarintToInteger(input, result);
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, int16_t &result) {
	return VarintToInteger(input, result);
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, int32_t &result) {
	return VarintToInteger(input, result);
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, int64_t &result) {
	return VarintToInteger(input, result);
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, uint8_t &result) {
	return VarintToInteger(input, result);
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, uint16_t &result) {
	return VarintToInteger(input, result);
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, uint32_t &result) {
	return VarintToInteger(input, result);
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, uint64_t &result) {
	return VarintToInteger(input, result);
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, hugeint_t &result) {
	return VarintToInteger(input, result);
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, uhugeint_t &result) {
	return VarintToInteger(input, result);
}

template <class T>
bool VarintToFloatingPoint(varint_t &input, T &result) {
	result = 0;

	auto blob_ptr = &input.value[0];

	// Determine if the number is negative
	bool is_negative = (blob_ptr[0] & 0x80) == 0;
	idx_t byte_pos = 0;
	for (idx_t i = input.value.size() - 1; i > 2; i--) {
		if (is_negative) {
			result += static_cast<uint8_t>(~blob_ptr[i]) * pow(256, static_cast<double>(byte_pos));
		} else {
			result += static_cast<uint8_t>(blob_ptr[i]) * pow(256, static_cast<double>(byte_pos));
		}
		byte_pos++;
	}

	if (is_negative) {
		result *= -1;
	}
	if (!std::isfinite(result)) {
		return false;
	}
	return true;
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, float &result) {
	return VarintToFloatingPoint(input, result);
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, double &result) {
	return VarintToFloatingPoint(input, result);
}
template <>
DUCKDB_API bool Varint::TryCast(varint_t input, long double &result) {
	return VarintToFloatingPoint(input, result);
}

template <class T>
varint_t IntegerToVarint(T int_value) {
	// Determine if the number is negative
	bool is_negative = int_value < 0;
	// Determine the number of data bytes
	uint64_t abs_value;
	if (is_negative) {
		if (int_value == std::numeric_limits<T>::min()) {
			abs_value = static_cast<uint64_t>(std::numeric_limits<T>::max()) + 1;
		} else {
			abs_value = static_cast<uint64_t>(std::abs(static_cast<int64_t>(int_value)));
		}
	} else {
		abs_value = static_cast<uint64_t>(int_value);
	}
	uint32_t data_byte_size;
	if (abs_value != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size = (abs_value == 0) ? 1 : static_cast<uint32_t>(std::ceil(std::log2(abs_value + 1) / 8.0));
	} else {
		data_byte_size = static_cast<uint32_t>(std::ceil(std::log2(abs_value) / 8.0));
	}
	varint_t result;

	uint32_t blob_size = data_byte_size + Varint::VARINT_HEADER_SIZE;
	result.value.reserve(blob_size);
	// auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = &result.value[0];
	Varint::SetHeader(writable_blob, data_byte_size, is_negative);

	// Add data bytes to the blob, starting off after header bytes
	idx_t wb_idx = Varint::VARINT_HEADER_SIZE;
	for (int i = static_cast<int>(data_byte_size) - 1; i >= 0; --i) {
		if (is_negative) {
			writable_blob[wb_idx++] = static_cast<char>(~(abs_value >> i * 8 & 0xFF));
		} else {
			writable_blob[wb_idx++] = static_cast<char>(abs_value >> i * 8 & 0xFF);
		}
	}
	return result;
}
template <>
bool Varint::TryConvert(int8_t value, varint_t &result) {
	result = IntegerToVarint(value);
	return true;
}
template <>
bool Varint::TryConvert(int16_t value, varint_t &result) {
	result = IntegerToVarint(value);
	return true;
}
template <>
bool Varint::TryConvert(int32_t value, varint_t &result) {
	result = IntegerToVarint(value);
	return true;
}
template <>
bool Varint::TryConvert(int64_t value, varint_t &result) {
	result = IntegerToVarint(value);
	return true;
}
template <>
bool Varint::TryConvert(uint8_t value, varint_t &result) {
	result = IntegerToVarint(value);
	return true;
}
template <>
bool Varint::TryConvert(uint16_t value, varint_t &result) {
	result = IntegerToVarint(value);
	return true;
}
template <>
bool Varint::TryConvert(uint32_t value, varint_t &result) {
	result = IntegerToVarint(value);
	return true;
}
template <>
bool Varint::TryConvert(uint64_t value, varint_t &result) {
	result = IntegerToVarint(value);
	return true;
}

template <>
bool Varint::TryConvert(uhugeint_t value, varint_t &result) {
	uint32_t data_byte_size;
	if (value.upper != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size = (value.upper == 0) ? 0 : static_cast<uint32_t>(std::ceil(std::log2(value.upper + 1) / 8.0));
	} else {
		data_byte_size = static_cast<uint32_t>(std::ceil(std::log2(value.upper) / 8.0));
	}

	uint32_t upper_byte_size = data_byte_size;
	if (data_byte_size > 0) {
		// If we have at least one byte on the upper side, the bottom side is complete
		data_byte_size += 8;
	} else {
		if (value.lower != NumericLimits<uint64_t>::Maximum()) {
			data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(value.lower + 1) / 8.0));
		} else {
			data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(value.lower) / 8.0));
		}
	}
	if (data_byte_size == 0) {
		data_byte_size++;
	}
	uint32_t blob_size = data_byte_size + Varint::VARINT_HEADER_SIZE;
	result.value.reserve(blob_size);
	auto writable_blob = &result.value[0];
	Varint::SetHeader(writable_blob, data_byte_size, false);

	// Add data bytes to the blob, starting off after header bytes
	idx_t wb_idx = Varint::VARINT_HEADER_SIZE;
	for (int i = static_cast<int>(upper_byte_size) - 1; i >= 0; --i) {
		writable_blob[wb_idx++] = static_cast<char>(value.upper >> i * 8 & 0xFF);
	}
	for (int i = static_cast<int>(data_byte_size - upper_byte_size) - 1; i >= 0; --i) {
		writable_blob[wb_idx++] = static_cast<char>(value.lower >> i * 8 & 0xFF);
	}
	return true;
}

template <>
bool Varint::TryConvert(hugeint_t value, varint_t &result) {
	// Determine if the number is negative
	bool is_negative = value.upper >> 63 & 1;
	if (is_negative) {
		// We must check if it's -170141183460469231731687303715884105728, since it's not possible to negate it
		// without overflowing
		if (value == NumericLimits<hugeint_t>::Minimum()) {
			varint_t cast_value;
			uhugeint_t u_int_value {0x8000000000000000, 0};
			Varint::TryConvert(u_int_value, cast_value);
			// We have to do all the bit flipping.
			auto writable_value_ptr = &result.value[0];
			result.value.resize(cast_value.value.size());
			Varint::SetHeader(writable_value_ptr, cast_value.value.size() - Varint::VARINT_HEADER_SIZE, is_negative);
			for (idx_t i = Varint::VARINT_HEADER_SIZE; i < cast_value.value.size(); i++) {
				writable_value_ptr[i] = static_cast<char>(~cast_value.value[i]);
			}
			return true;
		}
		value = -value;
	}
	// Determine the number of data bytes
	uint64_t abs_value_upper = static_cast<uint64_t>(value.upper);

	uint32_t data_byte_size;
	if (abs_value_upper != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size =
		    (abs_value_upper == 0) ? 0 : static_cast<uint32_t>(std::ceil(std::log2(abs_value_upper + 1) / 8.0));
	} else {
		data_byte_size = static_cast<uint32_t>(std::ceil(std::log2(abs_value_upper) / 8.0));
	}

	uint32_t upper_byte_size = data_byte_size;
	if (data_byte_size > 0) {
		// If we have at least one byte on the upper side, the bottom side is complete
		data_byte_size += 8;
	} else {
		if (value.lower != NumericLimits<uint64_t>::Maximum()) {
			data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(value.lower + 1) / 8.0));
		} else {
			data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(value.lower) / 8.0));
		}
	}

	if (data_byte_size == 0) {
		data_byte_size++;
	}
	uint32_t blob_size = data_byte_size + Varint::VARINT_HEADER_SIZE;
	result.value.reserve(blob_size);
	auto writable_blob = &result.value[0];
	Varint::SetHeader(writable_blob, data_byte_size, is_negative);

	// Add data bytes to the blob, starting off after header bytes
	idx_t wb_idx = Varint::VARINT_HEADER_SIZE;
	for (int i = static_cast<int>(upper_byte_size) - 1; i >= 0; --i) {
		if (is_negative) {
			writable_blob[wb_idx++] = static_cast<char>(~(abs_value_upper >> i * 8 & 0xFF));
		} else {
			writable_blob[wb_idx++] = static_cast<char>(abs_value_upper >> i * 8 & 0xFF);
		}
	}
	for (int i = static_cast<int>(data_byte_size - upper_byte_size) - 1; i >= 0; --i) {
		if (is_negative) {
			writable_blob[wb_idx++] = static_cast<char>(~(value.lower >> i * 8 & 0xFF));
		} else {
			writable_blob[wb_idx++] = static_cast<char>(value.lower >> i * 8 & 0xFF);
		}
	}
	return true;
}

template <class T>
bool DoubleToVarint(T double_value, varint_t &result) {
	// Check if we can cast it
	if (!std::isfinite(double_value)) {
		// We can't cast inf -inf nan
		return false;
	}
	// Determine if the number is negative
	bool is_negative = double_value < 0;
	// Determine the number of data bytes
	double abs_value = std::abs(double_value);
	if (abs_value == 0) {
		// Return Value 0
		uint32_t blob_size = 1 + Varint::VARINT_HEADER_SIZE;
		result.value.resize(blob_size);
		auto writable_blob = &result.value[0];
		Varint::SetHeader(writable_blob, 1, false);
		writable_blob[3] = 0;
		return true;
	}
	vector<char> value;
	while (abs_value > 0) {
		double quotient = abs_value / 256;
		double truncated = floor(quotient);
		uint8_t byte = static_cast<uint8_t>(abs_value - truncated * 256);
		abs_value = truncated;
		if (is_negative) {
			value.push_back(static_cast<char>(~byte));
		} else {
			value.push_back(static_cast<char>(byte));
		}
	}
	uint32_t data_byte_size = static_cast<uint32_t>(value.size());
	uint32_t blob_size = data_byte_size + Varint::VARINT_HEADER_SIZE;
	result.value.resize(blob_size);
	auto writable_blob = &result.value[0];
	Varint::SetHeader(writable_blob, data_byte_size, is_negative);
	// Add data bytes to the blob, starting off after header bytes
	idx_t blob_string_idx = value.size() - 1;
	for (idx_t i = Varint::VARINT_HEADER_SIZE; i < blob_size; i++) {
		writable_blob[i] = value[blob_string_idx--];
	}
	return true;
}

template <>
bool Varint::TryConvert(float value, varint_t &result) {
	return DoubleToVarint(value, result);
}
template <>
bool Varint::TryConvert(double value, varint_t &result) {
	return DoubleToVarint(value, result);
}
template <>
bool Varint::TryConvert(long double value, varint_t &result) {
	return DoubleToVarint(value, result);
}
template <>
bool Varint::TryConvert(const char *value, varint_t &result) {
	idx_t start_pos, end_pos;
	bool is_negative, is_zero;
	if (!VarcharFormatting(value, start_pos, end_pos, is_negative, is_zero)) {
		return false;
	}
	result.value = VarcharToVarInt(value);
	return true;
}

varint_t::varint_t(int64_t numeric_value) {
	Varint::TryConvert(numeric_value, *this);
}
varint_t::varint_t(hugeint_t input) {
	Varint::TryConvert(input, *this);
}

varint_t::varint_t(uhugeint_t input) {
	Varint::TryConvert(input, *this);
}

varint_t::varint_t(string_t input) {
	value = input.GetString();
}

// arithmetic operators
varint_t varint_t::operator+(const varint_t &rhs) const {
	return *this;
}
varint_t varint_t::operator-(const varint_t &rhs) const {
	return *this;
}
varint_t varint_t::operator*(const varint_t &rhs) const {
	return *this;
}
varint_t varint_t::operator/(const varint_t &rhs) const {
	return *this;
}
varint_t varint_t::operator%(const varint_t &rhs) const {
	return *this;
}
varint_t varint_t::operator-() const {
	return *this;
}

// bitwise operators
varint_t varint_t::operator>>(const varint_t &rhs) const {
	return *this;
}
varint_t varint_t::operator<<(const varint_t &rhs) const {
	return *this;
}
varint_t varint_t::operator&(const varint_t &rhs) const {
	return *this;
}
varint_t varint_t::operator|(const varint_t &rhs) const {
	return *this;
}
varint_t varint_t::operator^(const varint_t &rhs) const {
	return *this;
}
varint_t varint_t::operator~() const {
	return *this;
}

// in-place operators
varint_t &varint_t::operator+=(const varint_t &rhs) {
	return *this;
}
varint_t &varint_t::operator-=(const varint_t &rhs) {
	return *this;
}
varint_t &varint_t::operator*=(const varint_t &rhs) {
	return *this;
}
varint_t &varint_t::operator/=(const varint_t &rhs) {
	return *this;
}
varint_t &varint_t::operator%=(const varint_t &rhs) {
	return *this;
}
varint_t &varint_t::operator>>=(const varint_t &rhs) {
	return *this;
}
varint_t &varint_t::operator<<=(const varint_t &rhs) {
	return *this;
}
varint_t &varint_t::operator&=(const varint_t &rhs) {
	return *this;
}
varint_t &varint_t::operator|=(const varint_t &rhs) {
	return *this;
}
varint_t &varint_t::operator^=(const varint_t &rhs) {
	return *this;
}

// boolean operators
varint_t::operator bool() const {
	return true;
}
bool varint_t::operator!() const {
	return true;
}

} // namespace duckdb
