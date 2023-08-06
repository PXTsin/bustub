#include "storage/page/page_guard.h"
#include <utility>
#include "buffer/buffer_pool_manager.h"

namespace bustub {
BasicPageGuard::BasicPageGuard(BufferPoolManager *bpm, Page *page) : bpm_(bpm), page_(page) {
  printf("BasicPageGuard(BufferPoolManager *bpm, Page *page);\n");
}
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  printf("BasicPageGuard(BasicPageGuard &&that);\n");
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  printf("BasicPageGuard::Drop();\n");
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  printf("BasicPageGuard::operator=(BasicPageGuard &&that);\n");
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  printf("~BasicPageGuard();\n");
  Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  printf("ReadPageGuard(ReadPageGuard &&that);\n");
  guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  printf("ReadPageGuard::operator=(ReadPageGuard &&that);\n");
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  printf("ReadPageGuard::Drop();\n");
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  printf("~ReadPageGuard();\n");
  Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  printf("WritePageGuard(WritePageGuard &&that);\n");
  guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  printf("WritePageGuard::operator=(WritePageGuard &&that);\n");
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  printf("WritePageGuard::Drop();\n");
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  printf("~WritePageGuard();\n");
  Drop();
}  // NOLINT

}  // namespace bustub
