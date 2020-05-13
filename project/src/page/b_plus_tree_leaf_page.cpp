/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id)
{
  int max_size = (PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / sizeof(MappingType);
  SetPageType(IndexPageType::LEAF_PAGE);
  // first key is valid
  SetSize(0);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const
{
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) 
{
  next_page_id_ = next_page_id;
}


/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int BinarySearch(const MappingType *array, size_t size,
                 const KeyType &key, const KeyComparator &cmp)
{
  size_t start = 1, end = size - 1, middle = 0;

  while (start < end)
  {
    middle = start + ((end - start) >> 1);
    if (cmp(array[middle].first, key) < 0)
    {
      start = middle + 1;
    }
    else if (cmp(array[middle].first, key) > 0)
    {
      end = middle;
    }
    else
    {
      start = middle;
      break;
    }
  }
  return start;
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
  int size = GetSize();
  return BinarySearch(array, size, key, comparator);
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  KeyType key = array[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  int size = GetSize();
  assert(index < size);
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
  int size = GetSize();
  int insert_index = -1;
  std::pair<KeyType, ValueType> pair = std::make_pair(key, value);

  if (size == 0 || comparator(key, array[size-1].first) > 0) {
    insert_index = size;
  } else if (comparator(key, array[0].first) < 0) {
    memmove(array+1, array, size*sizeof(MappingType));
    insert_index = 0;
  } else {
    insert_index = KeyIndex(key, comparator);
    memmove(array+insert_index, array+insert_index+1, (size-insert_index)*sizeof(MappingType));
  }
  array[insert_index] = pair;
  IncreaseSize(1);
  return size+1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) 
{
  int size = GetSize();
  int half = (1 + size) >> 1;

  recipient->CopyHalfFrom(array + size - half, half);

  IncreaseSize(-1 * half);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size)
{
  int start = GetSize();
  assert(start == 0);
  memcpy(array+start, items, size*sizeof(MappingType));
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
  int size = GetSize();
  int index = BinarySearch(array, size, key, comparator);
  if (index == -1) {
    return false;
  } 
  value = array[index].second;
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
  int size = GetSize();
  int index = BinarySearch(array, size, key, comparator);
  if (index != -1) {
    memmove(array+index+1, array+index, (size-index-1)*sizeof(MappingType));
    IncreaseSize(-1);
  }
  return size;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *)
{
  int size = GetSize();
  recipient->CopyAllFrom(array, size);
  recipient->SetNextPageId(GetNextPageId());
  IncreaseSize(-1*size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size)
{
  int current_size = GetSize();
  memcpy(items, array+current_size, size*sizeof(MappingType));
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyLastFrom(array[0]);
  int size = GetSize();
  memmove(array, array+1, (size-1)*sizeof(MappingType));
  IncreaseSize(-1);

  page_id_t page_id = GetPageId();
  page_id_t parent_page_id = GetParentPageId();
  Page* parent_page = buffer_pool_manager->FetchPage(parent_page_id);
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *btree_internal_parent_page =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_page->GetData());
  int index_in_parent = btree_internal_parent_page->ValueIndex(page_id); 
  btree_internal_parent_page->SetKeyAt(index_in_parent, array[0].first);
  buffer_pool_manager->UnpinPage(parent_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item)
{
  int size = GetSize();
  array[size] = item;
  IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager)
{
  int size = GetSize();
  recipient->CopyFirstFrom(array[size-1], parentIndex, buffer_pool_manager);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager)
{
  int size = GetSize();
  memmove(array+1, array, size*sizeof(MappingType));
  IncreaseSize(1);
  array[0] = item;

  page_id_t parent_page_id = GetParentPageId();
  Page* parent_page = buffer_pool_manager->FetchPage(parent_page_id);
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *btree_internal_parent_page =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_page->GetData());
  btree_internal_parent_page->SetKeyAt(parentIndex, array[0].first);
  buffer_pool_manager->UnpinPage(parent_page_id, true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
