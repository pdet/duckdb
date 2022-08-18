//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

namespace duckdb {

class Leaf : public Node {

public:
	static SwizzleablePointer CreateLeaf(Key &value, unsigned depth, row_t row_id);
	static void Insert(SwizzleablePointer &leaf, row_t row_id);
	static void Remove(SwizzleablePointer &leaf, row_t row_id);
	static row_t GetRowId(SwizzleablePointer &leaf, idx_t index);
	BlockPointer Serialize(duckdb::MetaBlockWriter &writer);

	static Leaf *Deserialize(duckdb::MetaBlockReader &reader);

private:
	Leaf(row_t row_id);
	Leaf(Key &value, unsigned depth, row_t row_id);

	Leaf(unique_ptr<row_t[]> row_ids, idx_t num_elements, Prefix &prefix);

	void Insert(row_t row_id);
	void Remove(row_t row_id);
	row_t GetRowId(idx_t index) {
		return row_ids[index];
	}

	unique_ptr<row_t[]> row_ids;
	idx_t capacity;
};

} // namespace duckdb
