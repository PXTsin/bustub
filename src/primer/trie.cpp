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
  std::shared_ptr<const TrieNode> current = root_;
  std::size_t key_size = key.size();
  decltype(key_size) pos = 0;
  while (pos < key_size && current) {
    char ch = key[pos++];
    current = current->children_.find(ch) != current->children_.end() ? current->children_.at(ch) : nullptr;
  }
  if (pos != key_size || !current || !current->is_value_node_) {
    return nullptr;
  }
  auto res = dynamic_cast<const TrieNodeWithValue<T> *>(current.get());
  return res ? res->value_.get() : nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<T> shared_value = std::make_shared<T>(std::move(value));
  std::vector<std::shared_ptr<const TrieNode>> node_stack;  // store the same node
  std::shared_ptr<const TrieNode> cur_node = root_;
  std::size_t key_size = key.size();
  decltype(key_size) idx = 0;
  // 1.store the same node
  while (idx < key_size && cur_node) {
    char ch = key[idx++];
    node_stack.push_back(cur_node);
    cur_node = cur_node->children_.find(ch) != cur_node->children_.end() ? cur_node->children_.at(ch) : nullptr;
  }
  // 2.create diff node
  // 2.1create leaf node
  std::shared_ptr<const TrieNodeWithValue<T>> leaf_node =
      cur_node ? std::make_shared<const TrieNodeWithValue<T>>(cur_node->children_, shared_value)
               : std::make_shared<const TrieNodeWithValue<T>>(shared_value);
  // 2.2create diff inner node
  std::shared_ptr<const TrieNode> child_node = leaf_node;
  while (idx < key_size) {
    char ch = key[--key_size];
    std::map<char, std::shared_ptr<const TrieNode>> children{{ch, child_node}};
    cur_node = std::make_shared<const TrieNode>(children);
    child_node = cur_node;
  }
  // 3.copy same node
  cur_node = child_node;
  for (size_t i = node_stack.size() - 1; i < node_stack.size(); --i) {
    cur_node = std::shared_ptr<const TrieNode>(node_stack[i]->Clone());
    const_cast<TrieNode *>(cur_node.get())->children_[key[i]] = child_node;
    child_node = cur_node;
  }
  return Trie(cur_node);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::vector<std::shared_ptr<const TrieNode>> node_stack;  // store the same node
  std::shared_ptr<const TrieNode> cur_node = root_;
  std::size_t key_size = key.size();
  decltype(key_size) idx = 0;
  // 1.store the same node
  while (idx < key_size && cur_node) {
    char ch = key[idx++];
    node_stack.push_back(cur_node);
    cur_node = cur_node->children_.find(ch) != cur_node->children_.end() ? cur_node->children_.at(ch) : nullptr;
  }
  if (idx != key_size || !cur_node || !cur_node->is_value_node_) {
    return *this;
  }
  // 2.create end node
  std::shared_ptr<const TrieNode> end_node =
      cur_node->children_.empty() ? nullptr : std::make_shared<const TrieNode>(cur_node->children_);
  // 3.copy same node
  std::shared_ptr<const TrieNode> child_node = end_node;
  cur_node = end_node;
  for (size_t i = node_stack.size() - 1; i < node_stack.size(); --i) {
    cur_node = std::shared_ptr<const TrieNode>(node_stack[i]->Clone());
    const_cast<TrieNode *>(cur_node.get())->children_[key[i]] = child_node;
    child_node = cur_node;
  }
  return Trie(cur_node);
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
