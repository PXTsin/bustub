#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  page_id_t page_id = GetRootPageId();
  if (INVALID_PAGE_ID == page_id) {
    return true;
  }
  auto guard = bpm_->FetchPageBasic(page_id);
  auto page = reinterpret_cast<BPlusTreePage *>(guard.GetDataMut());
  return page->GetSize() <= 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPageLeaf(const KeyType &key, Context &ctx) -> page_id_t {
  page_id_t page_id = GetRootPageId();
  auto guard = bpm_->FetchPageWrite(page_id);
  auto page = reinterpret_cast<BPlusTreePage *>(guard.GetDataMut());
  ctx.write_set_.push_back(std::move(guard));
  /*找到叶子节点*/
  while (!page->IsLeafPage()) {
    auto page2 = reinterpret_cast<InternalPage *>(page);
    /*二分查找*/
    // int l = 0;
    // int h = page2->GetSize();
    // int index = (l + h) / 2;
    // for (; comparator_(key, page2->KeyAt(index)) != 0;) {
    //   if (index == h || index == l) {
    //     ++index;
    //     break;
    //   }
    //   if (comparator_(key, page2->KeyAt(index)) > 0) {
    //     l = index;
    //   } else {
    //     h = index;
    //   }
    //   index = (l + h) / 2;
    // }
    int index = 1;
    while (comparator_(page2->KeyAt(index), key) <= 0 && index < page2->GetSize()) {
      ++index;
    }
    if (index > 0) {
      --index;
    }
    page_id = page2->ValueAt(index);
    guard = bpm_->FetchPageWrite(page_id);
    page = reinterpret_cast<BPlusTreePage *>(guard.GetDataMut());
    ctx.write_set_.push_back(std::move(guard));
  }
  return page_id;
};
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  Context ctx;
  auto page_id = GetPageLeaf(key, ctx);
  auto page = reinterpret_cast<LeafPage *>(bpm_->FetchPageBasic(page_id).GetDataMut());
  int index = page->FindKeyIndex(key, comparator_);
  if (index == -1) {
    return false;
  }
  result->push_back(page->ValueAt(index));
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  auto page = bpm_->NewPageGuarded(&page_id);
  if (page.GetData() == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  auto *leaf = reinterpret_cast<LeafPage *>(page.GetDataMut());
  leaf->Init(leaf_max_size_);
  leaf->InsertAt(key, value, comparator_);
  leaf->SetNextPageId(INVALID_PAGE_ID);
  /*set root_page_id_*/
  auto page2 = reinterpret_cast<BPlusTreeHeaderPage *>(bpm_->FetchPageBasic(header_page_id_).GetDataMut());
  page2->root_page_id_ = page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  Context ctx;
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  ctx.root_page_id_ = ctx.header_page_->PageId();
  GetPageLeaf(key, ctx);
  auto *node = ctx.write_set_.back().AsMut<LeafPage>();
  auto size = node->GetSize();
  auto new_size = node->InsertAt(key, value, comparator_);
  /*key相同*/
  if (new_size == size) {
    return false;
  }
  /*页节点没满*/
  if (new_size < leaf_max_size_) {
    return true;
  }
  /*页满了，需要分页*/
  page_id_t page_id;
  auto sibling_leaf_node = Split(node, &page_id);
  sibling_leaf_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(page_id);

  auto risen_key = sibling_leaf_node->KeyAt(0);
  InsertIntoParent(node, risen_key, sibling_leaf_node, ctx, transaction);
  auto header = reinterpret_cast<BPlusTreeHeaderPage *>(ctx.header_page_->GetDataMut());
  header->root_page_id_ = ctx.root_page_id_;
  ctx.header_page_ = std::nullopt;
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Context &ctx, Transaction *transaction) {
  /*根页面*/
  if (ctx.write_set_.size() == 1) {
    auto root_page_guard = bpm_->NewPageGuarded(&ctx.root_page_id_);
    auto *root_page = reinterpret_cast<InternalPage *>(root_page_guard.GetDataMut());
    if (root_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    }
    auto old_node_id = reinterpret_cast<Page *>(old_node)->GetPageId();
    auto new_node_id = reinterpret_cast<Page *>(new_node)->GetPageId();
    root_page->Init(leaf_max_size_);
    root_page->SetKeyAt(1, key);
    root_page->SetValueAt(0, old_node_id);
    root_page->SetValueAt(1, new_node_id);
    root_page->SetSize(2);
    return;
  }
  // auto old_node_id = reinterpret_cast<Page *>(old_node)->GetPageId();
  auto new_node_id = reinterpret_cast<Page *>(new_node)->GetPageId();
  ctx.write_set_.pop_back();
  auto *parent_page = reinterpret_cast<InternalPage *>(ctx.write_set_.back().GetDataMut());
  /*父页面不需要分页*/
  if (parent_page->GetSize() < internal_max_size_) {
    /*insert*/
    parent_page->InsertAt(key, new_node_id, comparator_);
    return;
  }
  auto temp = static_cast<InternalPage *>(malloc(BUSTUB_PAGE_SIZE + sizeof(MappingType)));
  temp->InitData(parent_page->GetData(), 0, internal_max_size_);
  temp->InsertAt(key, new_node_id, comparator_);
  /*create new page*/
  page_id_t page_id;
  auto parent_sibling_page = reinterpret_cast<InternalPage *>(bpm_->NewPageGuarded(&page_id).GetDataMut());
  parent_sibling_page->Init(internal_max_size_);
  /*分页成L1和L2*/
  parent_page->InitData(temp->GetData(), 0, (internal_max_size_ + 1) / 2);
  parent_sibling_page->InitData(temp->GetData(), (internal_max_size_ + 1) / 2, internal_max_size_ + 1);
  auto new_key = parent_sibling_page->KeyAt(0);
  InsertIntoParent(parent_page, new_key, parent_sibling_page, ctx, transaction);
  free(temp);
}
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node, page_id_t *page_id) -> N * {
  auto page_tmp = bpm_->NewPage(page_id);
  if (page_tmp == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  auto *page = reinterpret_cast<N *>(page_tmp->GetData());
  if (node->IsLeafPage()) {
    page->Init(leaf_max_size_);
    page->InitData(node->GetData(), (leaf_max_size_ + 1) / 2, leaf_max_size_);
    node->InitData(node->GetData(), 0, (leaf_max_size_ + 1) / 2);
  } else {
    page->Init(internal_max_size_);
    page->InitData(node->GetData(), (internal_max_size_ + 1) / 2, internal_max_size_ + 1);
    node->InitData(node->GetData(), 0, (internal_max_size_ + 1) / 2);
  }
  return page;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  printf("tree.Insert()");
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  Context ctx;
  GetPageLeaf(key, ctx);
  auto page_tmp = ctx.write_set_.back().AsMut<LeafPage>();

  /*不用分页*/
  if (page_tmp->GetSize() < leaf_max_size_ - 1) {
    /*insert in leaf*/
    return page_tmp->InsertAt(key, value, comparator_);
  }
  /*分页*/
  if (page_tmp->GetSize() >= leaf_max_size_ - 1) {
    page_tmp->InsertAt(key, value, comparator_);
    /*create new page*/
    page_id_t page_id{};
    auto page_guard = bpm_->NewPageGuarded(&page_id);
    auto page = reinterpret_cast<LeafPage *>(page_guard.GetDataMut());
    page->Init(leaf_max_size_);
    /*分页成L1和L2*/
    page_tmp->InitData(page_tmp->GetData(), 0, (leaf_max_size_ + 1) / 2);
    page->InitData(page_tmp->GetData(), (leaf_max_size_ + 1) / 2, leaf_max_size_);
    /*!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!l2.next_page_id=l1.next_page_id,l1.next_page_id=l2*/
    page->SetNextPageId(page_tmp->GetNextPageId());
    page_tmp->SetNextPageId(page_id);

    /*将L2第一个key插入到父节点,若父节点满了，则父节点分页*/
    ctx.write_set_.pop_back();
    /*根页面分裂，创建新的根节点*/
    if (ctx.write_set_.empty()) {
      page_id_t page_id1 = GetRootPageId();
      page_id_t page_id2 = page_id;
      page_id_t root_page_id{};
      auto root_page_guard = bpm_->NewPageGuarded(&root_page_id);
      auto root_page = reinterpret_cast<InternalPage *>(root_page_guard.GetDataMut());
      root_page->Init(internal_max_size_);

      root_page->SetKeyAt(0, page_tmp->KeyAt(0));
      root_page->SetKeyAt(1, page->KeyAt(0));
      root_page->SetValueAt(0, page_id1);
      root_page->SetValueAt(1, page_id2);
      root_page->SetSize(2);
      root_page_guard.Drop();

      auto header_page = reinterpret_cast<BPlusTreeHeaderPage *>(bpm_->FetchPageBasic(header_page_id_).GetDataMut());
      header_page->root_page_id_ = root_page_id;
      return true;
    }
    auto parent = reinterpret_cast<InternalPage *>(ctx.write_set_.back().GetDataMut());
    auto new_key = page->KeyAt(0);
    page_guard.Drop();
    /*父页面不需要分页*/
    if (parent->GetSize() < internal_max_size_) {
      parent->InsertAt(new_key, page_id, comparator_);
      return true;
    }
    /*父页面要分页*/
    while (parent->GetSize() == internal_max_size_) {
      auto temp = static_cast<InternalPage *>(malloc(BUSTUB_PAGE_SIZE + sizeof(MappingType)));
      temp->InitData(parent->GetData(), 0, internal_max_size_);
      temp->InsertAt(new_key, page_id, comparator_);
      /*create new page*/
      auto page_guard = bpm_->NewPageGuarded(&page_id);
      auto page = reinterpret_cast<InternalPage *>(page_guard.GetDataMut());
      page->Init(internal_max_size_);
      /*分页成L1和L2*/
      parent->InitData(temp->GetData(), 0, (internal_max_size_ + 1) / 2);
      page->InitData(temp->GetData(), (internal_max_size_ + 1) / 2, internal_max_size_ + 1);
      free(temp);

      auto old_key = parent->KeyAt(0);
      new_key = page->KeyAt(0);

      ctx.write_set_.pop_back();
      page_guard.Drop();

      /*根页面分裂，创建新的根节点*/
      if (ctx.write_set_.empty()) {
        page_id_t page_id1 = GetRootPageId();
        page_id_t page_id2 = page_id;
        page_id_t root_page_id{};
        auto root_page_guard = bpm_->NewPageGuarded(&root_page_id);
        auto root_page = reinterpret_cast<InternalPage *>(root_page_guard.GetDataMut());
        root_page->Init(internal_max_size_);

        root_page->SetKeyAt(0, old_key);
        root_page->SetKeyAt(1, new_key);
        root_page->SetValueAt(0, page_id1);
        root_page->SetValueAt(1, page_id2);
        root_page->SetSize(2);
        root_page_guard.Drop();

        auto header_page = reinterpret_cast<BPlusTreeHeaderPage *>(bpm_->FetchPageBasic(header_page_id_).GetDataMut());
        header_page->root_page_id_ = root_page_id;
        return true;
      }
      parent = reinterpret_cast<InternalPage *>(ctx.write_set_.back().GetDataMut());
      if (parent->GetSize() < internal_max_size_) {
        parent->InsertAt(new_key, page_id, comparator_);
        return true;
      }
    }
  }

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  auto guard = bpm_->FetchPageBasic(header_page_id_);
  auto page = guard.AsMut<BPlusTreeHeaderPage>();
  return page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Dump2Name() {
  Draw(bpm_, "/test/atestcpp/picture.txt");
  system("dot -Tpng /test/atestcpp/picture.txt > /test/atestcpp/mytree.png");
}
/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
