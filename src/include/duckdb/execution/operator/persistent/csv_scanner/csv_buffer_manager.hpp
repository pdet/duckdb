//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_scanner/csv_buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/csv_scanner/csv_buffer.hpp"

namespace duckdb {

//! This class is used to manage the buffers
//! Buffers are cached when used for auto detection
//! Otherwise they are not cached and just returned
class CSVBufferManager{
public:
	CSVBufferManager(ClientContext &context, CSVFileHandle &file_handle);
	//! Returns a buffer from a buffer id (starting from 0). If it's in the auto-detection then we cache new buffers
	//! Otherwise we remove them from the cache if they are already there, or just return them bypassing the cache.
	shared_ptr<CSVBuffer> GetBuffer(idx_t pos, bool auto_detection);

private:
	//! Reads next buffer in reference to cached_buffers.front()
	bool ReadNextAndCacheIt();
	vector<shared_ptr<CSVBuffer>> cached_buffers;
	shared_ptr<CSVBuffer> last_buffer;
	idx_t global_csv_pos = 0;
	ClientContext &context;
	CSVFileHandle &file_handle;
};
}
