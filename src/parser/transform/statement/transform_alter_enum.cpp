#include "duckdb/parser/parsed_data/alter_enum_info.hpp"
#include "duckdb/parser/statement/alter_enum_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/alter_enum_info.hpp"

namespace duckdb {
string ReadTypeName(duckdb_libpgquery::PGList *PG_type_name) {
	for (auto c = PG_type_name->head; c != nullptr; c = lnext(c)) {
		auto node = reinterpret_cast<duckdb_libpgquery::PGList *>(c->data.ptr_value);
		switch (node->type) {
		case duckdb_libpgquery::T_PGString: {
			auto *cell = reinterpret_cast<duckdb_libpgquery::PGListCell *>(node);
			auto *def_elem = reinterpret_cast<duckdb_libpgquery::PGDefElem *>(cell->data.ptr_value);
			auto *format_val = (duckdb_libpgquery::PGValue *)(def_elem->arg);
			return format_val->val.str;
			//				auto cdef = (duckdb_libpgquery::PGColumnDef *)c->data.ptr_value;
			//				auto centry = TransformColumnDefinition(cdef);
			//				if (cdef->constraints) {
			//					for (auto constr = cdef->constraints->head; constr != nullptr; constr = constr->next) {
			//						auto constraint = TransformConstraint(constr, centry, info->columns.size());
			//						if (constraint) {
			//							info->constraints.push_back(move(constraint));
			//						}
			//					}
			//				}
			//				info->columns.push_back(move(centry));
			break;
		}
		case duckdb_libpgquery::T_PGConstraint: {
			//				info->constraints.push_back(TransformConstraint(c));
			break;
		}
		default:
			throw NotImplementedException("ColumnDef type not handled yet");
		}
	}
}
unique_ptr<AlterEnumStatement> Transformer::TransformAlterEnum(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGAlterEnumStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_unique<AlterEnumStatement>();
	auto info = make_unique<AlterEnumInfo>();
	info->type_name = ReadTypeName(stmt->typeName); // stmt->typeName; why is this a list?
	if (stmt->newVal) {
		info->new_value = stmt->newVal;
	}
	if (stmt->oldVal) {
		info->old_value = stmt->oldVal;
	}
	if (stmt->newValNeighbor) {
		info->new_value_neighbor = stmt->newValNeighbor;
	}

	info->new_value_is_after = stmt->newValIsAfter;
	info->skip_if_new_value_exists = stmt->skipIfNewValExists;

	result->info = move(info);
	return result;
}
} // namespace duckdb