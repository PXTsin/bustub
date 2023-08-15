//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetData() -> MappingType * { return array_; }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InitData(MappingType *arr, int l, int h) {
  int n = h - l;
  SetSize(n);
  for (int i = 0; i < n; ++i) {
    array_[i] = arr[l + i];
  }
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(const KeyType &key, const ValueType &value,
                                              const KeyComparator &comparator) -> bool {
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
  // array_[index]=MappingType(key,value);
  // SetSize(GetSize()+1);
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
  for (int i = GetSize(); i > index; --i) {
    array_[i] = array_[i - 1];
  }
  array_[index] = MappingType(key, value);
  IncreaseSize(1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
