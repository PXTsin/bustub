/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int page_index)
    : page_id_(page_id), page_index_(page_index), bpm_(bpm) {
  if (page_id != INVALID_PAGE_ID) {
    page_guard_ = bpm_->FetchPageBasic(page_id_);
    leaf_page_ = page_guard_.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  page_guard_.Drop();
  page_id_ = INVALID_PAGE_ID;
  leaf_page_ = nullptr;
  bpm_ = nullptr;
  page_index_ = 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return static_cast<bool>(page_id_ == INVALID_PAGE_ID); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_page_->KeyValueAt(page_index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (page_id_ == INVALID_PAGE_ID) {
    throw std::runtime_error("operator++超过范围\n");
  }
  if (page_index_ < leaf_page_->GetSize() - 1) {
    ++page_index_;
    return *this;
  }
  page_id_ = leaf_page_->GetNextPageId();
  page_index_ = 0;
  if (page_id_ != INVALID_PAGE_ID) {
    page_guard_.Drop();
    page_guard_ = bpm_->FetchPageBasic(page_id_);
    leaf_page_ = page_guard_.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  }
  return *this;
}
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return static_cast<bool>(page_id_ == itr.page_id_ && page_index_ == itr.page_index_ && bpm_ == itr.bpm_);
}
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  return !(static_cast<bool>(page_id_ == itr.page_id_ && page_index_ == itr.page_index_ && bpm_ == itr.bpm_));
}
template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
