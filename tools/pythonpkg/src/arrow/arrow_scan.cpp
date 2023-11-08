
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
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
	auto gil = make_uniq<PythonGILWrapper>();
	{
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

		auto stream_factory =
		    make_uniq<PythonTableArrowArrayStreamFactory>(arrow_table.ptr(), bind_data.client_properties);

		// I think we can release the gil gere
		//		gil.reset();
		ArrowStreamParameters parameters;
		parameters.projected_columns.columns = {"a"};
		parameters.projected_columns.projection_map = {{0, "a"}};
		parameters.filters = nullptr;
		auto stream_wrapper = stream_factory->Produce((uintptr_t)stream_factory.get(), parameters);

		// We read the schema here to avoid a switcharoo
		ArrowSchemaWrapper schema;
		stream_wrapper->GetSchema(schema);
		ArrowTableType arrow_table_type;
		vector<string> names;
		vector<LogicalType> return_types;
		ArrowTableFunction::PopulateArrowTableType(arrow_table_type, schema, names, return_types);
		D_ASSERT(return_types.size() == 1);

		auto array = stream_wrapper->GetNextChunkUnique();
		ArrowScanLocalState arrow_local(std::move(array));
		ArrowArrayScanState scan_state(arrow_local);
		scan_state.state.column_ids = {0};

		// now we can build the scanner
		ArrowConverter::ColumnArrowToDuckDB(out, *scan_state.state.chunk->arrow_array.children[0], scan_state, count,
		                                    *arrow_table_type.GetColumns().at(0));
	}
}

} // namespace duckdb
