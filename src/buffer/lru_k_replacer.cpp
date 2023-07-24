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
#include <iostream>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  auto func = [&frame_id, this](LRUKNode *tail) {
    auto temp = tail;
    /*找到第一个可以放逐的frame*/
    while (temp != nullptr && !temp->is_evictable_) {
      temp = temp->front_;
    }
    if (temp == nullptr) {
      return false;
    }
    latch_.lock();
    /*更新链表头或尾*/
    if (temp == history_head_) {
      history_head_ = temp->next_;
    }
    if (temp == history_tail_) {
      history_tail_ = temp->front_;
    }
    if (temp == cache_head_) {
      cache_head_ = temp->next_;
    }
    if (temp == cache_tail_) {
      cache_tail_ = temp->front_;
    }
    ////////////////////
    /*从链表中摘除该节点*/
    if (temp->front_ != nullptr) {
      temp->front_->next_ = temp->next_;
    }
    if (temp->next_ != nullptr) {
      temp->next_->front_ = temp->front_;
    }
    --curr_size_;
    latch_.unlock();
    temp->k_ = 0;
    /////////////////
    *frame_id = temp->fid_;
    // Remove(temp->fid_);
    return true;
  };
  if (cache_head_ == nullptr) {  // cache_store_.empty()
    return func(history_tail_);
  }
  return func(cache_tail_);
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame id is invalid");
  /*在缓存中*/
  if (cache_store_[frame_id] != nullptr) {
    auto frame = cache_store_[frame_id];
    latch_.lock();
    if (cache_tail_ == frame) {
      cache_tail_ = frame->front_;
    }
    if (frame->front_ != nullptr) {
      frame->front_->next_ = frame->next_;
    }
    if (frame->next_ != nullptr) {
      frame->next_->front_ = frame->front_;
    }
    /*访问后放到链表头部*/
    cache_head_->front_ = frame;
    frame->next_ = cache_head_;
    frame->front_ = nullptr;
    cache_head_ = frame;
    cache_head_->front_ = nullptr;
    frame->k_++;
    latch_.unlock();
    return;
  }
  /*在历史存储中*/
  if (history_store_[frame_id] != nullptr) {
    auto frame = history_store_[frame_id];
    latch_.lock();
    if (history_tail_ == frame) {
      history_tail_ = frame->front_;
    }
    if (frame->front_ != nullptr) {
      frame->front_->next_ = frame->next_;
    }
    if (frame->next_ != nullptr) {
      frame->next_->front_ = frame->front_;
    }
    /*达到k次后，放入到缓存中*/
    if (++frame->k_ == k_) {
      cache_store_[frame_id] = frame;
      history_store_.erase(frame_id);
      frame->next_ = cache_head_;
      frame->front_ = nullptr;
      // if (cache_head_ != nullptr) {
      //   cache_head_->front_ = frame;
      // }else{
      //   cache_tail_=frame;
      // }
      cache_head_->front_ = frame;

      cache_head_ = frame;
      return;
    }
    /*访问后放到链表头部*/
    history_head_->front_ = frame;
    frame->next_ = history_head_;
    history_head_ = frame;
    history_head_->front_ = nullptr;
    latch_.unlock();
    return;
  }
  /*没有记录就创建*/
  auto node = new LRUKNode();
  node->fid_ = frame_id;
  node->k_ = 1;
  latch_.lock();
  history_store_[frame_id] = node;
  /**/
  if (history_head_ == nullptr) {
    history_head_ = node;
    history_tail_ = node;
    latch_.unlock();
    return;
  }
  history_head_->front_ = node;
  node->next_ = history_head_;
  history_head_ = node;
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  auto temp = (cache_store_[frame_id] != nullptr) ? cache_store_[frame_id] : history_store_[frame_id];
  if (temp == nullptr) {
    latch_.unlock();
    return;
  }
  temp->fid_ = frame_id;
  curr_size_ = set_evictable ? curr_size_ + static_cast<size_t>(set_evictable ^ temp->is_evictable_)
                             : curr_size_ - static_cast<size_t>(set_evictable ^ temp->is_evictable_);
  temp->is_evictable_ = set_evictable;
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (cache_store_[frame_id] != nullptr) {
    auto temp = cache_store_[frame_id];
    cache_store_.erase(frame_id);
    delete temp;
  } else if (history_store_[frame_id] != nullptr) {
    auto temp = history_store_[frame_id];
    history_store_.erase(frame_id);
    delete temp;
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
