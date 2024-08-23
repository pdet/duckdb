//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_converter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

struct ArrowConverter {
	DUCKDB_API static void ToArrowSchema(ArrowSchema *out_schema, const vector<LogicalType> &types,
	                                     const vector<string> &names, const ClientProperties &options, const column_binding_map_t<unique_ptr<BaseStatistics>> &root_statistics);
	DUCKDB_API static void ToArrowArray(DataChunk &input, ArrowArray *out_array, ClientProperties options);
};

} // namespace duckdb
