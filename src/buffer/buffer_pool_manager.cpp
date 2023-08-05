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
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <utility>

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
auto BufferPoolManager::FindReplace() -> frame_id_t * {
  frame_id_t *fid = nullptr;
  if (!free_list_.empty()) {
    if (fid != nullptr) {
      *fid = free_list_.front();
    }
    free_list_.pop_front();
  } else { /*从replacer取*/
    if (replacer_->Evict(fid)) {
      if (fid != nullptr) {
        page_table_.erase(pages_[*fid].page_id_);
      }
    } else {
      return nullptr;
    }
  }
  return fid;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lk(latch_);
  frame_id_t fid;
  page_id_t pid;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else { /*从replacer取*/
    if (replacer_->Evict(&fid)) {
      page_id_t t = pages_[fid].page_id_;
      page_table_.erase(t);
    } else {
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
  if (pages_[fid].IsDirty()) {
    disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].GetData());
    pages_[fid].ResetMemory();
    pages_[fid].is_dirty_ = false;
  }
  pages_[fid].pin_count_ = 1;
  pages_[fid].page_id_ = pid;
  return &pages_[fid];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lk(latch_);
  frame_id_t fid = 0;
  if (page_table_.count(page_id) > 0) {
    fid = page_table_[page_id];
    replacer_->RecordAccess(fid);
    pages_[fid].pin_count_++;
  } else {
    if (!free_list_.empty()) {
      fid = free_list_.front();
      free_list_.pop_front();
    } else {
      if (replacer_->Evict(&fid)) {
        page_table_.erase(pages_[fid].page_id_);
      } else {
        return nullptr;
      }
    }
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    page_table_[page_id] = fid;
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].GetData());
      pages_[fid].ResetMemory();
      pages_[fid].is_dirty_ = false;
    }
    pages_[fid].pin_count_ = 1;
    pages_[fid].page_id_ = page_id;
  }
  disk_manager_->ReadPage(page_id, pages_[fid].GetData());
  return &pages_[fid];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  frame_id_t fid = page_table_[page_id];
  if (page_table_.count(page_id) == 0 || pages_[fid].GetPinCount() <= 0) {
    return false;
  }
  if (is_dirty) {
    pages_[fid].is_dirty_ = is_dirty;
  }
  if (--pages_[fid].pin_count_ == 0) {
    replacer_->SetEvictable(fid, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t fid = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[fid].GetData());
  pages_[fid].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lk(latch_);
  for (auto &e : page_table_) {
    disk_manager_->WritePage(e.first, pages_[e.second].GetData());
    pages_[e.second].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t fid = page_table_[page_id];
  if (pages_[fid].GetPinCount() > 0) {
    return false;
  }
  replacer_->Remove(fid);
  free_list_.emplace_back(fid);
  page_table_.erase(page_id);
  pages_[fid].ResetMemory();
  pages_[fid].pin_count_ = 0;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  std::lock_guard<std::mutex> lk(latch_);
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  std::lock_guard<std::mutex> lk(latch_);
  pages_[page_table_[page_id]].pin_count_++;
  return {this, &pages_[page_table_[page_id]]};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  std::lock_guard<std::mutex> lk(latch_);
  pages_[page_table_[page_id]].RLatch();
  pages_[page_table_[page_id]].pin_count_++;
  return {this, &pages_[page_table_[page_id]]};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  std::lock_guard<std::mutex> lk(latch_);
  pages_[page_table_[page_id]].WLatch();
  pages_[page_table_[page_id]].pin_count_++;
  return {this, &pages_[page_table_[page_id]]};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  std::lock_guard<std::mutex> lk(latch_);
  return {this, NewPage(page_id)};
}

}  // namespace bustub
