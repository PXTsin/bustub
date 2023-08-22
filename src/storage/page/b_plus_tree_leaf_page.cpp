//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
}
/*[l,h)*/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InitData(MappingType *arr, int l, int h) {
  int n = h - l;
  SetSize(n);
  for (int i = 0; i < n; ++i) {
    array_[i] = arr[l + i];
  }
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetData() -> MappingType * { return array_; }
/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetFrontPageId() const -> page_id_t { return front_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetFrontPageId(page_id_t front_page_id) { front_page_id_ = front_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyValueAt(int index) -> const MappingType & { return array_[index]; }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindKeyIndex(const KeyType &key, const KeyComparator &comparator) -> int {
  int l = 0;
  int h = GetSize();
  int i = (l + h) / 2;
  for (; comparator(key, KeyAt(i)) != 0;) {
    if (i == h || i == l) {
      i = -1;
      break;
    }
    if (comparator(key, KeyAt(i)) > 0) {
      l = i;
    } else {
      h = i;
    }
    i = (l + h) / 2;
  }
  return i;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  int index = 0;
  while (comparator(KeyAt(index), key) < 0 && index < GetSize()) {
    ++index;
  }
  if (comparator(KeyAt(index), key) != 0) {
    return false;
  }
  std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  // int l = 0;
  // int h = GetSize();
  // int index = (l + h) / 2;
  // for (; comparator(key, KeyAt(index)) != 0;) {
  //   if (index == h || index == l) {
  //     index++;
  //     break;
  //   }
  //   if (comparator(key, KeyAt(index)) > 0) {
  //     l = index;
  //   } else {
  //     h = index;
  //   }
  //   index = (l + h) / 2;
  // }
  // for(int i=GetSize();i>index;--i){
  //   array_[i]=array_[i-1];
  // }
  if (GetSize() == 0) {
    array_[0] = MappingType(key, value);
    IncreaseSize(1);
    return true;
  }
  int index = 0;
  printf("comparator(KeyAt(index), key)=%d\n", comparator(KeyAt(index), key));
  while (comparator(KeyAt(index), key) < 0 && index < GetSize()) {
    ++index;
  }
  std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  array_[index] = MappingType(key, value);
  IncreaseSize(1);
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
