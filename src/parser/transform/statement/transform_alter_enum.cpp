#include "duckdb/parser/parsed_data/alter_enum_info.hpp"
#include "duckdb/parser/statement/alter_enum_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/alter_enum_info.hpp"

namespace duckdb {
	unique_ptr<AlterEnumStatement> Transformer::TransformAlterEnum(duckdb_libpgquery::PGNode *node) {
		auto stmt = reinterpret_cast<duckdb_libpgquery::PGAlterEnumStmt *>(node);
		D_ASSERT(stmt);
		auto result = make_unique<AlterEnumStatement>();
		auto info = make_unique<AlterEnumInfo>();
		info->type_name = stmt->typeName; // why is this a list?
	    info->new_value = stmt->newVal;
	    info->old_value = stmt->oldVal;
	    info->new_value_is_after = stmt->newValIsAfter;
	    info->skip_if_new_value_exists = stmt->skipIfNewValExists;
	    info->new_value_neighbor = stmt->newValNeighbor;
		vector<string> ordered_array = ReadPgListToString(stmt->vals);
		info->type = make_unique<LogicalType>(LogicalType::ENUM(info->name, ordered_array));
		result->info = move(info);
		return result;
	}
}