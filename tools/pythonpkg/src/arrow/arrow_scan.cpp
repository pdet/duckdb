//#include "duckdb_python/pyrelation.hpp"
//#include "duckdb_python/pyconnection/pyconnection.hpp"
//#include "duckdb_python/pyresult.hpp"
//#include "duckdb_python/python_conversion.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
//#include "utf8proc_wrapper.hpp"
//#include "duckdb/common/case_insensitive_map.hpp"
//#include "duckdb_python/pandas/pandas_bind.hpp"
//#include "duckdb_python/numpy/numpy_type.hpp"
//#include "duckdb_python/pandas/pandas_analyzer.hpp"
//#include "duckdb_python/numpy/numpy_type.hpp"
//#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb_python/arrow/arrow_scan.hpp"
#include "duckdb_python/pandas/column/pandas_arrow_column.hpp"
#include "duckdb_python/pandas/pandas_bind.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace duckdb {
// 'offset' is the offset within the column
// 'count' is the amount of values we will convert in this batch
void ArrowScan::Scan(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out) {
	D_ASSERT(bind_data.pandas_col->Backend() == PandasColumnBackend::ARROW);
	auto &arrow_col = reinterpret_cast<PandasArrowColumn &>(*bind_data.pandas_col);
	auto &chunked_array = arrow_col.array;
	// We have to turn the chunked array into something we can export to c
	// For now let's do a table, but I think we can directly do a scanner
	//	pyarrow.Table.from_arrays([df['a'].array._pa_array], names = ['a'])
	// Turn chunked_array into Arrow Array
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	auto arrow_table_class = import_cache.pyarrow.Table();
	vector<py::object> column {chunked_array};
	// fixme: use correct name?
	vector<string> column_name {"a"};

	py::list column_list = py::cast(column);
	py::list column_name_list = py::cast(column_name);
	auto arrow_table = arrow_table_class.attr("from_arrays")(column_list, column_name_list);
	auto export_to_c = arrow_table.attr("__arrow_c_stream__");
	ArrowArrayStreamWrapper stream_wrapper;
	export_to_c(reinterpret_cast<uint64_t>(&stream_wrapper.arrow_array_stream));

	auto array = stream_wrapper.GetNextChunkUnique();
	ArrowScanLocalState arrow_local(std::move(array));
	ArrowArrayScanState scan_state(arrow_local);
	// now we can build the scanner
	switch (bind_data.numpy_type.type) {
	case NumpyNullableType::OBJECT:
		ArrowConverter::ColumnArrowToDuckDB(out, arrow_local.chunk->arrow_array, scan_state, count,
		                                    ArrowType(LogicalType::VARCHAR));
		break;
	default:
		throw NotImplementedException("Unsupported Pandas-Arrow type");
	}
}

} // namespace duckdb
