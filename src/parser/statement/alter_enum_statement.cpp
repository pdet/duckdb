#include "duckdb/parser/statement/alter_enum_statement.hpp"

namespace duckdb {

AlterEnumStatement::AlterEnumStatement() : SQLStatement(StatementType::ALTER_ENUM_STATEMENT) {
}

AlterEnumStatement::AlterEnumStatement(unique_ptr<AlterEnumInfo> info)
    : SQLStatement(StatementType::ALTER_ENUM_STATEMENT), info(move(info)) {
}

unique_ptr<SQLStatement> AlterEnumStatement::Copy() const {
	auto result = make_unique<AlterEnumStatement>(info->Copy());
	return move(result);
}

} // namespace duckdb
