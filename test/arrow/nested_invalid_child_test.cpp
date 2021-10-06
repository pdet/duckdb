#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow_test_factory.hpp"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow_check.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include <parquet/arrow/reader.h>
#include "arrow/io/file.h"
#include <arrow/type_traits.h>
#include "arrow/table.h"
#include "arrow/c/bridge.h"
#include <memory>
#include <iostream>
#include "parquet/exception.h"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/query_result.hpp"
#include "test_helpers.hpp"

TEST_CASE("Test Throw on Nested Type Children", "[arrow]") {
	duckdb::DuckDB db;
	duckdb::Connection conn {db};
	// Create table with nested type where one of the children has no conversion to arrow
	// This should be currently leaking
	auto result = conn.Query("SELECT {'a':id, 'b':id,'i':uid} FROM (SELECT [1] as id, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::UUID as uid) as t");
	ArrowSchema abi_arrow_schema;
	REQUIRE_THROWS(result->ToArrowSchema(&abi_arrow_schema));
}