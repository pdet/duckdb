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
namespace duckdb {

	struct AlterEnumInfo : public ParseInfo {
		AlterEnumInfo() {
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

//	public:
//		CatalogType GetCatalogType() override {
//			return CatalogType::TABLE_ENTRY;
//		}
//		void Serialize(Serializer &serializer) override;
//		static unique_ptr<AlterInfo> Deserialize(Deserializer &source);
	};
}
