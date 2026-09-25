//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/collection_merger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

enum class RowGroupBatchType : uint8_t { FLUSHED, NOT_FLUSHED };

class CollectionMerger {
public:
	explicit CollectionMerger(ClientContext &context, DataTable &data_table)
	    : context(context), data_table(data_table), batch_type(RowGroupBatchType::NOT_FLUSHED) {
	}

	struct MergeSource {
		PhysicalIndex collection_index = PhysicalIndex(DConstants::INVALID_INDEX);
		unique_ptr<ColumnDataCollection> chunks;
	};

	//! The transaction context.
	ClientContext &context;
	//! The data table.
	DataTable &data_table;
	//! Sources in batch order
	vector<MergeSource> sources;
	//! The batch type for merging collections.
	RowGroupBatchType batch_type;

public:
	void AddCollection(const PhysicalIndex collection_index, RowGroupBatchType type) {
		if (batch_type == RowGroupBatchType::FLUSHED || (type == RowGroupBatchType::FLUSHED && !Empty())) {
			throw InternalException("Cannot merge flushed collections");
		}
		MergeSource source;
		source.collection_index = collection_index;
		sources.push_back(std::move(source));
		batch_type = type;
	}

	void AddChunks(unique_ptr<ColumnDataCollection> chunks) {
		if (batch_type == RowGroupBatchType::FLUSHED) {
			throw InternalException("Cannot merge buffered chunks into a flushed collection");
		}
		MergeSource source;
		source.chunks = std::move(chunks);
		sources.push_back(std::move(source));
	}

	bool Empty() {
		return sources.empty();
	}

	PhysicalIndex Flush(OptimisticDataWriter &writer) {
		if (Empty()) {
			return PhysicalIndex(DConstants::INVALID_INDEX);
		}

		// Only reuse the first collection to preserve batch order
		idx_t first_append_index = 1;
		auto result_collection_index = sources[0].collection_index;
		if (!result_collection_index.IsValid()) {
			auto new_collection = writer.CreateCollection(data_table, sources[0].chunks->Types());
			new_collection->collection->InitializeEmpty();
			result_collection_index = data_table.CreateOptimisticCollection(context, std::move(new_collection));
			first_append_index = 0;
		}
		auto &optimistic_collection = data_table.GetOptimisticCollection(context, result_collection_index);
		auto &result_collection = *optimistic_collection.collection;

		if (sources.size() > first_append_index) {
			auto &types = result_collection.GetTypes();
			TableAppendState append_state;
			result_collection.InitializeAppend(append_state);

			DataChunk scan_chunk;
			scan_chunk.Initialize(context, types);

			vector<StorageIndex> column_ids;
			for (idx_t i = 0; i < types.size(); i++) {
				column_ids.emplace_back(i);
			}
			for (idx_t i = first_append_index; i < sources.size(); i++) {
				auto &source = sources[i];
				if (source.chunks) {
					writer.Append(optimistic_collection, *source.chunks, append_state);
					source.chunks.reset();
					continue;
				}
				auto &collection = data_table.GetOptimisticCollection(context, source.collection_index);
				TableScanState scan_state;
				scan_state.Initialize(column_ids);
				collection.collection->InitializeScan(context, scan_state.local_state, column_ids, nullptr);

				while (true) {
					scan_chunk.Reset();
					scan_state.local_state.Scan(scan_chunk, TableScanType::TABLE_SCAN_ALL_ROWS);
					if (scan_chunk.size() == 0) {
						break;
					}
					writer.Append(optimistic_collection, scan_chunk, append_state);
				}
				data_table.ResetOptimisticCollection(context, source.collection_index);
			}
			result_collection.FinalizeAppend(TransactionData::Unversioned(), append_state);
			writer.WriteUnflushedRowGroups(optimistic_collection);
		} else if (batch_type == RowGroupBatchType::NOT_FLUSHED) {
			writer.WriteUnflushedRowGroups(optimistic_collection);
		}

		sources.clear();
		return result_collection_index;
	}
};

} // namespace duckdb
