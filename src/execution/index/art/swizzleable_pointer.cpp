#include "duckdb/execution/index/art/swizzleable_pointer.hpp"

namespace duckdb {
SwizzleablePointer::~SwizzleablePointer() {
	Reset();
}
uint64_t ShiftBit(idx_t bit_pos, uint64_t pointer) {
	idx_t pointer_size = sizeof(pointer) * 8;
	uint64_t mask = 1;
	mask = mask << (pointer_size - bit_pos);
	pointer |= mask;
	return pointer;
}
SwizzleablePointer::SwizzleablePointer(row_t row_id) {
	pointer = row_id;
	// Set the second most but to indicate this is a single node leaf
	pointer = ShiftBit(2, pointer);
}
SwizzleablePointer::SwizzleablePointer(duckdb::MetaBlockReader &reader) {
	idx_t block_id = reader.Read<block_id_t>();
	idx_t offset = reader.Read<uint32_t>();
	if (block_id == DConstants::INVALID_INDEX || offset == DConstants::INVALID_INDEX) {
		pointer = 0;
		return;
	}
	idx_t pointer_size = sizeof(pointer) * 8;
	pointer = block_id;
	pointer = pointer << (pointer_size / 2);
	pointer += offset;
	// Set the left most bit to indicate this is a swizzled pointer and send it back to the mother-ship
	pointer = ShiftBit(1, pointer);
}

SwizzleablePointer &SwizzleablePointer::operator=(const Node *ptr) {
	Reset();
	if (sizeof(ptr) == 4) {
		pointer = (uint32_t)(size_t)ptr;
	} else {
		pointer = (uint64_t)ptr;
	}
	return *this;
}

bool operator!=(const SwizzleablePointer &s_ptr, const uint64_t &ptr) {
	return (s_ptr.pointer != ptr);
}

BlockPointer SwizzleablePointer::GetSwizzledBlockInfo() {
	D_ASSERT(IsSwizzled());
	idx_t pointer_size = sizeof(pointer) * 8;
	pointer = pointer & ~(1ULL << (pointer_size - 1));
	uint32_t block_id = pointer >> (pointer_size / 2);
	uint32_t offset = pointer & 0xffffffff;
	return {block_id, offset};
}

bool IsBitSet(idx_t bit_pos, uint64_t pointer) {
	idx_t pointer_size = sizeof(pointer) * 8;
	return (pointer >> (pointer_size - bit_pos)) & 1;
}
bool SwizzleablePointer::IsSwizzled() {
	return IsBitSet(1, pointer);
}

bool SwizzleablePointer::IsUniqueLeaf() {
	return IsBitSet(2, pointer);
}

row_t SwizzleablePointer::GetRowID() {
	idx_t pointer_size = sizeof(pointer) * 8;
	return pointer & ~(1ULL << (pointer_size - 2));
}

void SwizzleablePointer::Reset() {
	if (pointer) {
		if (!IsSwizzled() && !IsUniqueLeaf()) {
			delete (Node *)pointer;
		}
	}
	*this = nullptr;
}

Node *SwizzleablePointer::Unswizzle(ART &art) {
	if (IsSwizzled()) {
		// This means our pointer is not yet in memory, gotta deserialize this
		// first we unset the bae
		auto block_info = GetSwizzledBlockInfo();
		*this = Node::Deserialize(art, block_info.block_id, block_info.offset);
	}
	return (Node *)pointer;
}

BlockPointer SwizzleablePointer::Serialize(ART &art, duckdb::MetaBlockWriter &writer) {
	if (pointer) {
		Unswizzle(art);
		return ((Node *)pointer)->Serialize(art, writer);
	} else {
		return {(block_id_t)DConstants::INVALID_INDEX, (uint32_t)DConstants::INVALID_INDEX};
	}
}
} // namespace duckdb
