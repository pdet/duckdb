#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

vector<LogicalType> ArrowTableType::GetDuckDBTypes() {
	vector<LogicalType> duckdb_types;
	for (auto &column : columns) {
		duckdb_types.emplace_back(column.type);
	}
	return duckdb_types;
}

void ArrowType::AddChild(ArrowType &child) {
	children.emplace_back(child);
}

LogicalType &ArrowType::GetDuckType() {
	return type;
}

void ArrowTableType::AddColumn(ArrowType &column) {
	columns.emplace_back(column);
}

static ArrowType GetArrowLogicalType(ArrowSchema &schema) {
	auto format = string(schema.format);
	if (format == "n") {
		return {LogicalType::SQLNULL};
	} else if (format == "b") {
		return {LogicalType::BOOLEAN};
	} else if (format == "c") {
		return {LogicalType::TINYINT};
	} else if (format == "s") {
		return {LogicalType::SMALLINT};
	} else if (format == "i") {
		return {LogicalType::INTEGER};
	} else if (format == "l") {
		return {LogicalType::BIGINT};
	} else if (format == "C") {
		return {LogicalType::UTINYINT};
	} else if (format == "S") {
		return {LogicalType::USMALLINT};
	} else if (format == "I") {
		return {LogicalType::UINTEGER};
	} else if (format == "L") {
		return {LogicalType::UBIGINT};
	} else if (format == "f") {
		return {LogicalType::FLOAT};
	} else if (format == "g") {
		return {LogicalType::DOUBLE};
	} else if (format[0] == 'd') { //! this can be either decimal128 or decimal 256 (e.g., d:38,0)
		std::string parameters = format.substr(format.find(':'));
		uint8_t width = std::stoi(parameters.substr(1, parameters.find(',')));
		uint8_t scale = std::stoi(parameters.substr(parameters.find(',') + 1));
		if (width > 38) {
			throw NotImplementedException("Unsupported Internal Arrow Type for Decimal %s", format);
		}
		return {LogicalType::DECIMAL(width, scale)};
	} else if (format == "u") {
		return {LogicalType::VARCHAR, ArrowVariableSizeType::NORMAL};
	} else if (format == "U") {
		return {LogicalType::VARCHAR, ArrowVariableSizeType::SUPER_SIZE};
	} else if (format == "tsn:") {
		return {LogicalTypeId::TIMESTAMP_NS};
	} else if (format == "tsu:") {
		return {LogicalTypeId::TIMESTAMP};
	} else if (format == "tsm:") {
		return {LogicalTypeId::TIMESTAMP_MS};
	} else if (format == "tss:") {
		return {LogicalTypeId::TIMESTAMP_SEC};
	} else if (format == "tdD") {
		return {LogicalType::DATE, ArrowDateTimeType::DAYS};
	} else if (format == "tdm") {
		return {LogicalType::DATE, ArrowDateTimeType::MILLISECONDS};
	} else if (format == "tts") {
		return {LogicalType::TIME, ArrowDateTimeType::SECONDS};
	} else if (format == "ttm") {
		return {LogicalType::TIME, ArrowDateTimeType::MILLISECONDS};
	} else if (format == "ttu") {
		return {LogicalType::TIME, ArrowDateTimeType::MICROSECONDS};
	} else if (format == "ttn") {
		return {LogicalType::TIME, ArrowDateTimeType::NANOSECONDS};
	} else if (format == "tDs") {
		return {LogicalType::INTERVAL, ArrowDateTimeType::SECONDS};
	} else if (format == "tDm") {
		return {LogicalType::INTERVAL, ArrowDateTimeType::MILLISECONDS};
	} else if (format == "tDu") {
		return {LogicalType::INTERVAL, ArrowDateTimeType::MICROSECONDS};
	} else if (format == "tDn") {
		return {LogicalType::INTERVAL, ArrowDateTimeType::NANOSECONDS};
	} else if (format == "tiD") {
		return {LogicalType::INTERVAL, ArrowDateTimeType::DAYS};
	} else if (format == "tiM") {
		return {LogicalType::INTERVAL, ArrowDateTimeType::MONTHS};
	} else if (format == "+l") {

		arrow_convert_data[col_idx]->variable_sz_type.emplace_back(ArrowVariableSizeType::NORMAL, 0);
		auto child_type = GetArrowLogicalType(*schema.children[0]);
		auto list_type = ... return LogicalType::LIST(child_type);
	} else if (format == "+L") {
		arrow_convert_data[col_idx]->variable_sz_type.emplace_back(ArrowVariableSizeType::SUPER_SIZE, 0);
		auto child_type = GetArrowLogicalType(*schema.children[0], arrow_convert_data, col_idx);
		return LogicalType::LIST(child_type);
	} else if (format[0] == '+' && format[1] == 'w') {
		std::string parameters = format.substr(format.find(':') + 1);
		idx_t fixed_size = std::stoi(parameters);
		arrow_convert_data[col_idx]->variable_sz_type.emplace_back(ArrowVariableSizeType::FIXED_SIZE, fixed_size);
		auto child_type = GetArrowLogicalType(*schema.children[0], arrow_convert_data, col_idx);
		return LogicalType::LIST(move(child_type));
	} else if (format == "+s") {
		child_list_t<LogicalType> child_types;
		for (idx_t type_idx = 0; type_idx < (idx_t)schema.n_children; type_idx++) {
			auto child_type = GetArrowLogicalType(*schema.children[type_idx], arrow_convert_data, col_idx);
			child_types.emplace_back(schema.children[type_idx]->name, child_type);
		}
		return LogicalType::STRUCT(move(child_types));

	} else if (format == "+m") {
		child_list_t<LogicalType> child_types;
		//! First type will be struct, so we skip it
		auto &struct_schema = *schema.children[0];
		for (idx_t type_idx = 0; type_idx < (idx_t)struct_schema.n_children; type_idx++) {
			//! The other types must be added on lists
			auto child_type = GetArrowLogicalType(*struct_schema.children[type_idx], arrow_convert_data, col_idx);

			auto list_type = LogicalType::LIST(child_type);
			child_types.emplace_back(struct_schema.children[type_idx]->name, list_type);
		}
		return LogicalType::MAP(move(child_types));
	} else if (format == "z") {
		return {LogicalType::BLOB, ArrowVariableSizeType::NORMAL};
	} else if (format == "Z") {
		return {LogicalType::BLOB, ArrowVariableSizeType::SUPER_SIZE};
	} else if (format[0] == 'w') {
		std::string parameters = format.substr(format.find(':') + 1);
		idx_t fixed_size = std::stoi(parameters);
		return {LogicalType::BLOB, fixed_size};
	} else if (format[0] == 't' && format[1] == 's') {
		// Timestamp with Timezone
		// TODO right now we just get the UTC value. We probably want to support this properly in the future
		if (format[2] == 'n') {
			return {LogicalType::TIMESTAMP_TZ, ArrowDateTimeType::NANOSECONDS};
		} else if (format[2] == 'u') {
			return {LogicalType::TIMESTAMP_TZ, ArrowDateTimeType::MICROSECONDS};
		} else if (format[2] == 'm') {
			return {LogicalType::TIMESTAMP_TZ, ArrowDateTimeType::MILLISECONDS};
		} else if (format[2] == 's') {
			return {LogicalType::TIMESTAMP_TZ, ArrowDateTimeType::SECONDS};
		} else {
			throw NotImplementedException(" Timestamptz precision of not accepted");
		}
	} else {
		throw NotImplementedException("Unsupported Internal Arrow Type %s", format);
	}
}
} // namespace duckdb
