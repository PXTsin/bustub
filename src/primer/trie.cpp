#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  std::shared_ptr<const TrieNode> current_ = root_;
  std::size_t key_size = key.size();
  decltype(key_size) pos = 0;
  while (pos < key_size && current_) {
    char ch = key[pos++];
    current_ = current_->children_.find(ch) != current_->children_.end() ? current_->children_.at(ch) : nullptr;
  }
  if (pos != key_size || !current_ || !current_->is_value_node_) {
    return nullptr;
  }
  const TrieNodeWithValue<T> *res = dynamic_cast<const TrieNodeWithValue<T> *>(current_.get());
  return res ? res->value_.get() : nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  std::shared_ptr<TrieNode> root = std::move(root_->Clone());
  std::shared_ptr<TrieNode> current_ = root;
  std::shared_ptr<TrieNode> prev_ = current_;
  int pos = 0;
  while (pos < (int)key.length()) {
    auto it = current_->children_.find(key[pos]);
    if (it != current_->children_.end()) {
      prev_ = current_;
      current_ = std::move(it->second->Clone());
      it->second = current_;
      ++pos;
    } else
      break;
  }
  if (pos == (int)key.length()) {  // 已存在
    const TrieNodeWithValue<T> *tmp = dynamic_cast<TrieNodeWithValue<T> *>(current_.get());
    //////////////////////
    // std::shared_ptr<TrieNodeWithValue<T>> res=std::move(tmp->Clone());
    std::shared_ptr<TrieNode> temp = std::move(tmp->Clone());
    TrieNodeWithValue<T> *temp2 = dynamic_cast<TrieNodeWithValue<T> *>(temp.get());
    std::shared_ptr<TrieNodeWithValue<T>> res = std::shared_ptr<TrieNodeWithValue<T>>(temp2);

    std::shared_ptr<T> temp3 = std::make_shared<T>(std::move(value));
    res->value_ = temp3;
    prev_->children_[key[pos - 1]] = res;
  } else {
    std::shared_ptr<TrieNode> prev = current_;
    while (pos < (int)key.length() - 1) {
      auto tmp = std::make_shared<TrieNode>();
      prev->children_[key[pos]] = tmp;
      prev = tmp;
      ++pos;
    }
    std::shared_ptr<T> value_temp = std::make_shared<T>(std::move(value));
    auto tmp = std::make_shared<TrieNodeWithValue<T>>(value_temp);
    prev->children_[key[pos]] = tmp;
  }
  return Trie(root);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  std::shared_ptr<TrieNode> current_ = std::move(root_->Clone());
  std::shared_ptr<TrieNode> prev = current_;
  int pos = 0;
  while (pos < (int)key.length()) {
    auto it = current_->children_.find(key[pos]);
    if (it == current_->children_.end()) {
      return *this;
    }
    prev = current_;
    current_ = std::move(it->second->Clone());
    ++pos;
  }
  if (current_->children_.empty()) {
    prev->children_.erase(key[pos - 1]);
  } else {
    std::shared_ptr<TrieNode> tmp = std::move(current_->Clone());
    tmp->is_value_node_ = false;
    prev->children_[key[pos - 1]] = tmp;
  }
  return *this;
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
