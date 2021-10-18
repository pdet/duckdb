//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/alter_enum_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/alter_enum_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class AlterEnumStatement : public SQLStatement {
public:
	AlterEnumStatement();
	explicit AlterEnumStatement(unique_ptr<AlterEnumInfo> info);
	unique_ptr<SQLStatement> Copy() const override;

	unique_ptr<AlterEnumInfo> info;
};

} // namespace duckdb
