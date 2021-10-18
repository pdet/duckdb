//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_enum_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"

namespace duckdb {

struct AlterEnumInfo : public ParseInfo {
	AlterEnumInfo() {
	}

	AlterEnumInfo(string type_name_p, string old_value_p, string new_value_p, string new_value_neighbor_p,
	              bool new_value_is_after_p, bool skip_if_new_value_exists_p)
	    : type_name(type_name_p), old_value(old_value_p), new_value_neighbor(new_value_neighbor_p),
	      new_value_is_after(new_value_is_after_p), skip_if_new_value_exists(skip_if_new_value_exists_p) {
	}
	//! qualified name
	string type_name;
	//!  old enum value's name, if renaming
	string old_value;
	//! new enum value's name
	string new_value;
	//! neighboring enum value, if specified
	string new_value_neighbor;
	//! place new enum value after neighbor
	bool new_value_is_after = false;
	//! no error if new already exists
	bool skip_if_new_value_exists = false;

public:
	//		CatalogType GetCatalogType() override {
	//			return CatalogType::TABLE_ENTRY;
	//		}
	unique_ptr<AlterEnumInfo> Copy() const {
		return make_unique<AlterEnumInfo>(type_name, old_value, new_value, new_value_neighbor, new_value_is_after,
		                                  skip_if_new_value_exists);
	};
	//		void Serialize(Serializer &serializer) override;
	//		static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
};
} // namespace duckdb
