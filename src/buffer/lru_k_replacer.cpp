//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <utility>
#include "common/config.h"
#include "common/exception.h"
#include "fmt/core.h"
#include "fmt/ostream.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
LRUKReplacer::~LRUKReplacer() { curr_size_ = 0; }
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  auto myevit = [this, &frame_id](auto e) {
    if (node_store_[e].is_evictable_) {
      *frame_id = e;
      if (node_store_[e].k_ < k_) {
        // node_less_k_.erase(node_store_[e].pos_);
        node_less_k_.erase(std::find(node_less_k_.begin(), node_less_k_.end(), node_store_[e].fid_));
      } else {
        // node_more_k_.erase(node_store_[e].pos_);
        node_more_k_.erase(std::find(node_more_k_.begin(), node_more_k_.end(), node_store_[e].fid_));
      }
      node_store_.erase(e);
      --curr_size_;
      return true;
    }
    return false;
  };
  return std::any_of(node_less_k_.begin(), node_less_k_.end(), myevit) ||
         std::any_of(node_more_k_.begin(), node_more_k_.end(), myevit);
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame id is invalid");
  std::lock_guard<std::mutex> lk(latch_);
  /*无记录，创建新记录*/
  if (node_store_.count(frame_id) == 0) {
    node_store_[frame_id] = LRUKNode();
    auto node = &node_store_[frame_id];
    node->fid_ = frame_id;
    node_less_k_.push_back(node->fid_);
    auto it = node_less_k_.begin();
    std::advance(it, node_less_k_.size() - 1);
    node->pos_ = it;
    return;
  }
  /*有记录*/
  auto node = &node_store_[frame_id];
  /*小于k*/
  if (node->k_ < k_) {
    /*访问后恰好为k*/
    if (++node->k_ == k_) {
      // node_less_k_.erase(node->pos_);
      node_less_k_.erase(std::find(node_less_k_.begin(), node_less_k_.end(), node->fid_));
      node_more_k_.push_back(node->fid_);
      auto it = node_more_k_.begin();
      std::advance(it, node_more_k_.size() - 1);
      node->pos_ = it;
      return;
    }
  } else { /*大于等于k*/
    node_more_k_.erase(std::find(node_more_k_.begin(), node_more_k_.end(), node->fid_));
    ++node->k_;
    node_more_k_.push_back(node->fid_);
    auto it = node_more_k_.begin();
    std::advance(it, node_more_k_.size() - 1);
    node->pos_ = it;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lk(latch_);
  if (set_evictable) {
    auto node = &node_store_[frame_id];
    if (!node->is_evictable_) {
      node->is_evictable_ = set_evictable;
      ++curr_size_;
    }
  } else {
    auto node = &node_store_[frame_id];
    if (node->is_evictable_) {
      node->is_evictable_ = set_evictable;
      --curr_size_;
    }
  }
}
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lk(latch_);
  if (node_store_.count(frame_id) > 0) {
    auto node = &node_store_[frame_id];
    if (node->is_evictable_) {
      --curr_size_;
      if (node->k_ < k_) {
        node_less_k_.erase(std::find(node_less_k_.begin(), node_less_k_.end(), frame_id));
      } else {
        node_more_k_.erase(std::find(node_more_k_.begin(), node_more_k_.end(), frame_id));
      }
      node_store_.erase(frame_id);
      return;
    }
    throw Exception("Remove is called on a non-evictable frame");
  }
}
auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
