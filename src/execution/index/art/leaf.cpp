#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include <cstring>

namespace duckdb {

SwizzleablePointer Leaf::CreateLeaf(Key &value, unsigned depth, row_t row_id) {
	if (value.len - depth == 0) {
		// This Leaf Has no Prefix, we can create a Single Value Leaf
		return SwizzleablePointer(row_id);
	}

	SwizzleablePointer ptr;
	ptr = new Leaf(value, depth, row_id);
	return ptr;
}

void Leaf::Insert(SwizzleablePointer &leaf, row_t row_id) {
	D_ASSERT(!leaf.IsSwizzled());
	if (leaf.IsUniqueLeaf()) {
		// We have to grow to a non-unique leaf
		auto new_leaf = new Leaf(row_id);
		leaf = new_leaf;
	} else {
		// We are in a non unique leaf, so we do an insert.
		auto leaf_ptr = (Leaf *)leaf.pointer;
		leaf_ptr->Insert(row_id);
	}
}
void Leaf::Remove(SwizzleablePointer &leaf, row_t row_id) {
	D_ASSERT(!leaf.IsSwizzled());
	if (leaf.IsUniqueLeaf()) {
		leaf.Reset();
	} else {
		// We are in a non unique leaf, so we do remove
		auto leaf_ptr = (Leaf *)leaf.pointer;
		leaf_ptr->Remove(row_id);
		if (leaf_ptr->count == 1) {
			// We turn this into a one leaf node
			auto remaining_row_id = leaf_ptr->GetRowId(0);
			// We have to grow to a non-unique leaf
			auto new_leaf = new Leaf(remaining_row_id);
			leaf = new_leaf;
		}
	}
}
row_t Leaf::GetRowId(SwizzleablePointer &leaf, idx_t index) {
	D_ASSERT(!leaf.IsSwizzled());
	if (leaf.IsUniqueLeaf()) {
		D_ASSERT(index == 0);
		return leaf.GetRowID();
	} else {
		// We are in a non unique leaf, so we do remove
		auto leaf_ptr = (Leaf *)leaf.pointer;
		return leaf_ptr->GetRowId(index);
	}
}

Leaf::Leaf(row_t row_id) : Node(NodeType::NLeaf) {
	capacity = 1;
	row_ids = unique_ptr<row_t[]>(new row_t[capacity]);
	row_ids[0] = row_id;
	count = 1;
}

Leaf::Leaf(Key &value, unsigned depth, row_t row_id) : Leaf(row_id) {
	D_ASSERT(value.len >= depth);
	prefix = Prefix(value, depth, value.len - depth);
}

Leaf::Leaf(unique_ptr<row_t[]> row_ids_p, idx_t num_elements_p, Prefix &prefix_p) : Node(NodeType::NLeaf) {
	capacity = num_elements_p;
	row_ids = move(row_ids_p);
	count = num_elements_p;
	prefix = prefix_p;
}

void Leaf::Insert(row_t row_id) {
	// Grow array
	if (count == capacity) {
		auto new_row_id = unique_ptr<row_t[]>(new row_t[capacity * 2]);
		memcpy(new_row_id.get(), row_ids.get(), capacity * sizeof(row_t));
		capacity *= 2;
		row_ids = move(new_row_id);
	}
	row_ids[count++] = row_id;
}

BlockPointer Leaf::Serialize(duckdb::MetaBlockWriter &writer) {
	auto block_id = writer.block->id;
	uint32_t offset = writer.offset;
	// Write Node Type
	writer.Write(type);
	// Write compression Info
	prefix.Serialize(writer);
	// Write Row Ids
	// Length
	writer.Write(count);
	// Actual Row Ids
	for (idx_t i = 0; i < count; i++) {
		writer.Write(row_ids[i]);
	}
	return {block_id, offset};
}

Leaf *Leaf::Deserialize(MetaBlockReader &reader) {
	Prefix prefix;
	prefix.Deserialize(reader);
	auto num_elements = reader.Read<uint16_t>();
	auto elements = unique_ptr<row_t[]>(new row_t[num_elements]);
	for (idx_t i = 0; i < num_elements; i++) {
		elements[i] = reader.Read<row_t>();
	}
	return new Leaf(move(elements), num_elements, prefix);
}

void Leaf::Remove(row_t row_id) {
	idx_t entry_offset = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < count; i++) {
		if (row_ids[i] == row_id) {
			entry_offset = i;
			break;
		}
	}
	if (entry_offset == DConstants::INVALID_INDEX) {
		return;
	}
	count--;
	if (capacity > 2 && count < capacity / 2) {
		// Shrink array, if less than half full
		auto new_row_id = unique_ptr<row_t[]>(new row_t[capacity / 2]);
		memcpy(new_row_id.get(), row_ids.get(), entry_offset * sizeof(row_t));
		memcpy(new_row_id.get() + entry_offset, row_ids.get() + entry_offset + 1,
		       (count - entry_offset) * sizeof(row_t));
		capacity /= 2;
		row_ids = move(new_row_id);
	} else {
		// Copy the rest
		for (idx_t j = entry_offset; j < count; j++) {
			row_ids[j] = row_ids[j + 1];
		}
	}
}

} // namespace duckdb
