#include "duckdb/execution/operator/csv_scanner/encode/csv_decoder.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

CSVDecoder::CSVDecoder(const CSVEncoding encoding_p) : encoding(encoding_p) {
}

bool CSVDecoder::IsUTF8() const {
	return encoding == CSVEncoding::UTF_8;
}

idx_t CSVDecoder::GetRatio() const {
	switch (encoding) {
	case CSVEncoding::UTF_8:
		return 1;
	case CSVEncoding::UTF_16:
	case CSVEncoding::LATIN_1:
		return 2;
	default:
		throw NotImplementedException("GetRatio() is not implemented for given CSVEncoding type");
	}
}

void CSVDecoder::DecodeUTF16(const char *encoded_buffer, idx_t encoded_buffer_size, char *decoded_buffer,
                             idx_t &decoded_buffer_start) {
	for (size_t i = 0; i < encoded_buffer_size; i += 2) {
		const uint16_t ch = static_cast<uint16_t>(static_cast<unsigned char>(encoded_buffer[i]) |
		                                          (static_cast<unsigned char>(encoded_buffer[i + 1]) << 8));

		if (ch >= 0xD800 && ch <= 0xDFFF) {
			throw InvalidInputException("CSV File is not utf-16 encoded");
		}
		if (ch <= 0x007F) {
			// 1-byte UTF-8 for ASCII characters
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(ch & 0x7F);
		} else if (ch <= 0x07FF) {
			// 2-byte UTF-8
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0xC0 | (ch >> 6));
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0x80 | (ch & 0x3F));
		} else {
			// 3-byte UTF-8
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0xE0 | (ch >> 12));
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0x80 | ((ch >> 6) & 0x3F));
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0x80 | (ch & 0x3F));
		}
	}
}

void CSVDecoder::DecodeLatin1(const char *encoded_buffer, const idx_t encoded_buffer_size, char *decoded_buffer,
                              idx_t &decoded_buffer_start) {
	for (size_t i = 0; i < encoded_buffer_size; i++) {
		const unsigned char ch = static_cast<unsigned char>(encoded_buffer[i]);
		if (ch > 0x7F && ch <= 0x9F) {
			throw InvalidInputException("CSV File is not latin-1 encoded");
		}
		if (ch <= 0x7F) {
			// ASCII: 1 byte in UTF-8
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(ch);
		} else {
			// Non-ASCII: 2 bytes in UTF-8
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0xC0 | (ch >> 6));
			decoded_buffer[decoded_buffer_start++] = static_cast<char>(0x80 | (ch & 0x3F));
		}
	}
}

void CSVDecoder::DecodeInternal(const char *encoded_buffer, const idx_t encoded_buffer_size, char *decoded_buffer,
                                idx_t &decoded_buffer_start) const {
	switch (encoding) {
	case CSVEncoding::UTF_8:
		throw InternalException("DecodeInternal() should not be called with UTF-8");
	case CSVEncoding::UTF_16:
		return DecodeUTF16(encoded_buffer, encoded_buffer_size, decoded_buffer, decoded_buffer_start);
	case CSVEncoding::LATIN_1:
		return DecodeLatin1(encoded_buffer, encoded_buffer_size, decoded_buffer, decoded_buffer_start);
	default:
		throw NotImplementedException("DecodeInternal() is not implemented for given CSVEncoding type");
	}
}

idx_t CSVDecoder::Decode(FileHandle &file_handle, void *buffer, const idx_t nr_bytes) const {
	idx_t ratio = GetRatio();
	idx_t encoded_bytes_to_read = nr_bytes / ratio;
	idx_t decoded_buffer_start = 0;
	// We could cache this
	std::unique_ptr<char[]> encoded_buffer(new char[encoded_bytes_to_read]);

	while (decoded_buffer_start < nr_bytes) {
		idx_t current_decoded_buffer_start = decoded_buffer_start;
		encoded_bytes_to_read = (nr_bytes - decoded_buffer_start) / ratio;
		auto actual_encoded_bytes = static_cast<idx_t>(file_handle.Read(encoded_buffer.get(), encoded_bytes_to_read));
		DecodeInternal(encoded_buffer.get(), actual_encoded_bytes, static_cast<char *>(buffer), decoded_buffer_start);
		if (decoded_buffer_start == current_decoded_buffer_start) {
			return decoded_buffer_start;
		}
	}
	return decoded_buffer_start;
}

} // namespace duckdb