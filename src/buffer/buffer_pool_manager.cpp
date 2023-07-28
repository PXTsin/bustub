//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstdlib>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t fid;
  page_id_t pid;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else { /*从replacer取*/
    if (!replacer_->Evict(&fid)) {
      return nullptr;
    }
  }
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  pid = AllocatePage();
  *page_id = pid;
  page_table_[pid] = fid;
  /*If the replacement frame has a dirty page,
   * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
   */
  /*判断是否为脏页并处理*/
  if (pages_[pid].IsDirty()) {
    disk_manager_->WritePage(pid, pages_[pid].GetData());
    pages_[pid].ResetMemory();
  }
  pages_[pid].pin_count_ = 1;
  return &pages_[pid];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  Page *p = &pages_[page_id];
  if (page_table_.count(page_id) > 0) {
  } else {
    frame_id_t fid;
    if (!free_list_.empty()) {
      fid = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Evict(&fid)) {
        return nullptr;
      }
    }
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    page_table_[page_id] = fid;
    if (pages_[page_id].IsDirty()) {
      disk_manager_->WritePage(page_id, pages_[page_id].GetData());
      pages_[page_id].ResetMemory();
    }
    disk_manager_->ReadPage(page_id, pages_[page_id].GetData());
  }
  p->pin_count_++;
  return p;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if (page_table_.count(page_id) == 0 || pages_[page_id].GetPinCount() <= 0) {
    return false;
  }
  pages_[page_id].is_dirty_ = is_dirty;
  if (--pages_[page_id].pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[page_id].GetData());
  pages_[page_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &e : page_table_) {
    disk_manager_->WritePage(e.first, pages_[e.first].GetData());
    pages_[e.first].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  if (pages_[page_id].GetPinCount() > 0) {
    return false;
  }
  replacer_->Remove(page_table_[page_id]);
  free_list_.emplace_back(page_table_[page_id]);
  page_table_.erase(page_id);
  pages_[page_id].ResetMemory();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return (next_page_id_++)%pool_size_; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, &pages_[page_id]}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  pages_[page_id].RLatch();
  return {this, &pages_[page_id]};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  pages_[page_id].WLatch();
  return {this, &pages_[page_id]};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
