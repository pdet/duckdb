//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/buffer_manager/csv_file_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {
class Allocator;
class FileSystem;

struct CSVFileHandle {
public:
	CSVFileHandle(FileSystem &fs, unique_ptr<FileHandle> file_handle_p, const string &path_p,
	              FileCompressionType compression);

	mutex main_mutex;

public:
	bool CanSeek();
	void Seek(idx_t position);
	bool OnDiskFile();

	idx_t FileSize();

	bool FinishedReading();

	idx_t Read(void *buffer, idx_t nr_bytes);

	string ReadLine();

	string GetFilePath();

	bool IsRemoteFile();

	static unique_ptr<FileHandle> OpenFileHandle(FileSystem &fs, const string &path, FileCompressionType compression);
	static unique_ptr<CSVFileHandle> OpenFile(FileSystem &fs, const string &path, FileCompressionType compression);
	bool uncompressed = false;

private:
	unique_ptr<FileHandle> file_handle;
	string path;
	bool can_seek = false;
	bool on_disk_file = false;
	idx_t file_size = 0;

	idx_t requested_bytes = 0;
	//! If we finished reading the file
	bool finished = false;
	bool is_remote_file = false;
};

} // namespace duckdb
