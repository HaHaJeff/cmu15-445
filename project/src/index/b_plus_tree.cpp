/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const
{
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction)
{
  Page* root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* btree_root_page = reinterpret_cast<BPlusTreePage*>(root_page);
  BPluseTreeLeafPage* btree_leaf_page = FindLeafPage(key, false); 
  ValueType value;

  if (btree_leaf_page->Lookup(key, value, comparator_)) {
    result.push_back(value);
  } else {
    return false;
  }
  buffer_pool_manager_->UnpinPage(btree_leaf_page->GetPageId(), false);
  return true;
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction)
{
  bool ret = true;
  if (IsEmpty()) {
    StartNewTree(key, value);
  } else {
    ret = InsertIntoLeaf(key, value);
  }
  return ret;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value)
{
  Page* page = buffer_pool_manager_->NewPage(root_page_id_);
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX, "out of memory");
  }
  BPluseTreeLeafPage* btree_leaf_page = reinterpret_cast<BPluseTreeLeafPage*>(page->GetData());
  UpdateRootPageId(true);
  btree_leaf_page->Init(root_page_id_);
  btree_leaf_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction)
{
  BPluseTreeLeafPage* btree_leaf_page = FindLeafPage(key, false);                                 
  ValueType tmp;

  // insert duplicate key
  if (btree_leaf_page->Lookup(key, tmp, comparator_)) {
    return false;
  } else {
    // safe node
    if (btree_leaf_page->GetSize() < btree_leaf_page->GetMaxSize()) {
      btree_leaf_page->Insert(key, value, comparator_);
    } else {
      BPluseTreeLeafPage* btree_new_laef_page = Split(btree_leaf_page);

      btree_new_laef_page->SetNextPageId(btree_leaf_page->GetNextPageId());
      btree_leaf_page->SetNextPageId(btree_new_leaf_page->GetPageId());
      KeyType mid_key = btree_new_laef_page->KeyAt(0);

      InsertIntoParent(btree_leaf_page, mid_key, btree_new_laef_page);

      // Insert into left half
      if (comparator_(key, mid_key) < 0) {
        btree_leaf_page->Insert(key, value, comparator_);
      } else { // Insert into right half
        btree_new_leaf_page->Insert(key, value, comparator_);
      }
      buffer_pool_manager_->UnpinPage(btree_new_laef_page->GetPageId(), true);
    }
  }
  buffer_pool_manager_->UnpinPage(btree_laef_page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node)
{
  page_id_t page_id = INVALID_PAGE_ID;
  Page* page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX, "out of memory");
  }
  N* new_node = reinterpret_cast<N*>(page->GetData());
  new_node->Init(page_id, node->GetParentPageId());
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node; 
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction)
{
  // also means old_root_page is full
  if (old_node->IsRootPage()) {
    // first generate new root_page
    Page* new_root_page = buffer_pool_manager_->NewPage(root_page_id_);
    if (new_root_page == nullptr) {
      throw Exception(EXCEPTION_TYPE_INDEX, "out of memory");
    }
    BPlusTreeInternalPage* btree_root_page = reinterpret_cast<BPlusTreeInternalPage*>(new_root_page->GetData());
    btree_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(false);
  } else { 
    Page* old_page = old_node->GetParentPageId();
    BPlusTreeInternalPage* btree_old_page = reinterpret_cast<BPlusTreeInternalPage*>(old_page->GetData());
    // parent page is not full
    if (btree_old_page->GetSize() < btree_parent_page->GetMaxSize()) {
      btree_old_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    } else {
      BPlusTreeInternalPage* btree_new_page = Split(btree_old_page);
      KeyType mid_key = btree_new_page->KeyAt(0);
      if (comparator_(key, mid_key) < 0) {
        btree_old_page->InsertNodeAfter(old_node.GetPageId(), key, new_old->GetPageId());
      } else {
        btree_new_page->InsertNodeAfter(old_node.GetPageId(), key, new_old->GetPageId());
        new_old->SetParentPageId(btree_new_page->GetPageId());
      }
      InsertIntoParent(btree_old_page, key, btree_new_page);
      buffer_pool_manager_->UnpinPage(btree_new_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(btree_old_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction)
{
  if (IsEmpty()) {
    return;
  }
  BPlusTreeLeafPage* btree_leaf_page = FindLeafPage(key, false);
  
  if (btree_leaf_page == nullptr) {
    return;
  }

  if (btree_leaef_page->GetSize() < btree_leaf_page->GetMinSize()) {
    CoalesceOrRedistribute(btree_leaf_page, transaction);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction)
{
  node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  if (parent->GetSize() < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  int size = old_root_page->GetSize();
  if (!old_root_page->IsLeafPage()) {
    if (size == 1) {
      BPlusTreeInternalPage* old_root_node = reinterpret_cast<BPlusTreeInternalPage*>(old_root_page);
      root_page_id_ = old_root_node->ValueAt(0);
      UpdateRootPageId(false);
      Page* new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
      BPlusTreePage* new_root_node = reinterpret_cast<BPlusTreePage*>(page->GetData());
      new_root_node->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true); 
      return true;
    }
  } else {
    if (old_root_page->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  return INDEXITERATOR_TYPE();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost)
{
  page_id_t page_id = root_page_id_;
  Page* page = buffer_pool_manager_->FetchPage(page_id);
  BPlusTreePage* btree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());

  while (page != nullptr && !btree_page->IsLeafPage()) {
    btree_page = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
    page_id_t old_page_id = page_id;
    if (leftMost == true) {
      page_id = btree_page->ValueAt(0);
    } else {
      page_id = btree_page->Loopup(key, comparator_);
    }
    buffer_pool_manager_->UnpinPage(old_page_id);
    page = buffer_pool_manager_->FetchPage(page_id);
  }
  BPlusTreeLeafPage* btree_leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(btree_page);
  return btree_leaf_page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
