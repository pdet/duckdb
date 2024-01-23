#include "duckdb/execution/operator/csv_scanner/buffer_manager/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/buffer_manager/csv_buffer.hpp"
#include "duckdb/function/table/read_csv.hpp"
namespace duckdb {

CSVBufferManager::CSVBufferManager(ClientContext &context_p, const CSVReaderOptions &options, const string &file_path_p,
                                   const idx_t file_idx_p, vector<shared_ptr<CSVBuffer>> recycled_buffers_p)
    : context(context_p), file_idx(file_idx_p), file_path(file_path_p), buffer_size(CSVBuffer::CSV_BUFFER_SIZE),
      recycled_buffers(recycled_buffers_p) {
	D_ASSERT(!file_path.empty());
	file_handle = ReadCSV::OpenCSV(file_path, options.compression, context);
	skip_rows = options.dialect_options.skip_rows.GetValue();
	auto file_size = file_handle->FileSize();
	if (file_size > 0 && file_size < buffer_size) {
		buffer_size = CSVBuffer::CSV_MINIMUM_BUFFER_SIZE;
	}
	if (options.buffer_size < buffer_size) {
		buffer_size = options.buffer_size;
	}
	Initialize();
}

void CSVBufferManager::UnpinBuffer(const idx_t cache_idx) {
	if (cache_idx < cached_buffers.size()) {
		cached_buffers[cache_idx]->Unpin();
	}
}

void CSVBufferManager::Initialize() {
	if (cached_buffers.empty()) {
		if (!recycled_buffers.empty()) {
			auto recycled_buffer = recycled_buffers.back();
			cached_buffers.emplace_back(
			    make_shared<CSVBuffer>(context, buffer_size, *file_handle, global_csv_pos, file_idx, recycled_buffer));
			recycled_buffers.pop_back();
		} else {
			cached_buffers.emplace_back(
			    make_shared<CSVBuffer>(context, buffer_size, *file_handle, global_csv_pos, file_idx));
		}

		last_buffer = cached_buffers.front();
	}
}

bool CSVBufferManager::ReadNextAndCacheIt(bool recycle) {
	D_ASSERT(last_buffer);
	for (idx_t i = 0; i < 2; i++) {
		if (!last_buffer->IsCSVFileLastBuffer()) {
			auto cur_buffer_size = buffer_size;
			shared_ptr<CSVBuffer> recycled_buffer;
			if (!recycled_buffers.empty()) {
				recycled_buffer = recycled_buffers.back();
				recycled_buffers.pop_back();
			}
			if (!recycled_buffer && recycle && cached_buffers.size() >2){
				recycled_buffer = cached_buffers[cached_buffers.size() - 3];
			}
			if (file_handle->uncompressed && !recycled_buffer) {
				if (file_handle->FileSize() - bytes_read) {
					cur_buffer_size = file_handle->FileSize() - bytes_read;
				}
			}
			if (cur_buffer_size == 0) {
				last_buffer->last_buffer = true;
				return false;
			}
			auto maybe_last_buffer = last_buffer->Next(*file_handle, cur_buffer_size, file_idx, recycled_buffer);
			if (!maybe_last_buffer) {
				last_buffer->last_buffer = true;
				return false;
			}
			last_buffer = std::move(maybe_last_buffer);
			bytes_read += last_buffer->GetBufferSize();
			cached_buffers.emplace_back(last_buffer);
			return true;
		}
	}
	return false;
}

unique_ptr<CSVBufferHandle> CSVBufferManager::GetBuffer(const idx_t pos, bool recycle) {
	lock_guard<mutex> parallel_lock(main_mutex);
	while (pos >= cached_buffers.size()) {
		if (done) {
			return nullptr;
		}
		if (!ReadNextAndCacheIt(recycle)) {
			done = true;
		}
	}
	if (pos != 0) {
		cached_buffers[pos - 1]->Unpin();
	}
	return cached_buffers[pos]->Pin(*file_handle);
}

idx_t CSVBufferManager::GetBufferSize() {
	return buffer_size;
}

idx_t CSVBufferManager::BufferCount() {
	return cached_buffers.size();
}

bool CSVBufferManager::Done() {
	return done;
}

string CSVBufferManager::GetFilePath() {
	return file_path;
}
vector<shared_ptr<CSVBuffer>> CSVBufferManager::GetRecycledBuffers() {
	if (cached_buffers.size() < 3){
		return cached_buffers;
	}
	vector<shared_ptr<CSVBuffer>> statiegeld;
	for (idx_t i = 0; i < 3; i ++){
		statiegeld.push_back(cached_buffers.back());
		cached_buffers.pop_back();
	}
	return statiegeld;
}

} // namespace duckdb
