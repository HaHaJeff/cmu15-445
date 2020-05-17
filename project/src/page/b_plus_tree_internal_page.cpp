/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb
{
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id)
{
  int max_size = (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / sizeof(MappingType);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  // first key is valid
  SetSize(0);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const
{
  // replace with your own code
  assert(0 <= index && index < GetSize());
  KeyType key = array[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key)
{
  assert(0 <= index && index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
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
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const
{
  int size = GetSize();
  int start = 0, end = size - 1, middle = 0;

  while (start <= end)
  {
    middle = start + ((end - start) >> 1);
    if (array[middle].second > value)
    {
      end = middle - 1;
    }
    else if (array[middle].second < value)
    {
      start = middle + 1;
    }
    else
    {
      break;
    }
  }
  return start <= end ? middle : -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const
{
  assert(0 <= index && index <= GetSize());
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const
{
  ValueType value;
  int size = GetSize();
  int left = 1, right = size - 1;
  if (comparator(key, array[left].first) < 0)
  {
    value = array[left - 1].second;
  }
  else if (comparator(key, array[right].first) > 0)
  {
    value = array[right].second;
  }
  else
  {
    int index = BinarySearch(array, size, key, comparator);
    assert(left <= index && index <= right);
    value = array[index - 1].second;
  }
  return value;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value)
{

  // Init page
  array[0].second = old_value;
  array[1] = std::make_pair(new_key, new_value);

  // Increase mount of records
  IncreaseSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value)
{
  int size = GetSize();
  int max_size = GetMaxSize();
  assert(size < max_size);

  int old_index = ValueIndex(old_value);

  int dest_index = old_index + 2, src_index = old_index + 1,
      num_bytes = (size - src_index) * sizeof(MappingType);
  memmove(array + dest_index, array + src_index, num_bytes);

  IncreaseSize(1);

  return size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
  int size = GetSize();
  int half = (1 + size) >> 1;

  recipient->CopyHalfFrom(array + size - half, half, buffer_pool_manager);

  IncreaseSize(-1 * half);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager)
{
  page_id_t page_id = this->GetPageId();
  // Start always = 0
  int start = GetSize();
  assert(start == 0);

  for (int i = 0; i < size; i++)
  {
    // Change parent_id of page
    Page *page = buffer_pool_manager->FetchPage(items[i].second);
    BPlusTreeInternalPage *btree_internal_page =
        reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    btree_internal_page->SetParentPageId(page_id);

    // Unpin the page and set it dirty
    buffer_pool_manager->UnpinPage(btree_internal_page->GetPageId(), true);

    array[i + start] = items[i];
  }

  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index)
{
  int size = GetSize();
  memmove(array + index, array + index + 1, (size - index - 1) * sizeof(MappingType));
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild()
{
  IncreaseSize(-1);
  return array[1].second;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager)
{
  page_id_t parent_page_id = GetParentPageId();
  int size = GetSize();

  // Valid first key of array
  Page *page = buffer_pool_manager->FetchPage(parent_page_id);
  BPlusTreeInternalPage *btree_internal_parent_page =
      reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  SetKeyAt(0, btree_internal_parent_page->KeyAt(index_in_parent));
  btree_internal_parent_page->Remove(index_in_parent);

  buffer_pool_manager->UnpinPage(parent_page_id, true);

  recipient->CopyAllFrom(array, size, buffer_pool_manager);
  IncreaseSize(-1 * size);
}

// Thu function used for merge node
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager)
{
  int current_size = GetSize();
  int max_size = GetMaxSize();
  assert(current_size + size <= max_size);
  page_id_t page_id = this->GetPageId();

  for (int i = 0; i < size; i++)
  {
    // Change parent_id of page
    Page *page = buffer_pool_manager->FetchPage(items[i].second);
    BPlusTreeInternalPage *btree_internal_page =
        reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    btree_internal_page->SetParentPageId(page_id);
    // Unpin the page and set it dirty
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
    array[i + current_size] = items[i];
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
  page_id_t id = GetPageId();
  page_id_t parent_id = GetParentPageId();

  Page *parent_page = buffer_pool_manager->FetchPage(parent_id);
  BPlusTreeInternalPage *btree_internal_parent_page = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());

  // Get the index of this page in parent->array
  int index_in_parent = btree_internal_parent_page->ValueIndex(id);

  // Get the key of index_in_parent
  KeyType key = btree_internal_parent_page->KeyAt(index_in_parent);
  std::pair<KeyType, ValueType> pair = std::make_pair(key, array[0].second);

  // update relavent key & value pair in its parent page
  btree_internal_parent_page->SetKeyAt(index_in_parent, array[1].first);
  buffer_pool_manager->UnpinPage(parent_id, true);

  // Remove the first key & value in array
  Remove(0);

  recipient->CopyLastFrom(pair, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager)
{
  // Insert into last of thie page
  int current_size = GetSize();
  array[current_size] = pair;
  IncreaseSize(1);

  page_id_t child_id = pair.second;
  page_id_t id = GetPageId();

  //
  Page *child_page = buffer_pool_manager->FetchPage(child_id);
  BPlusTreeInternalPage *btree_internal_child_page =
      reinterpret_cast<BPlusTreeInternalPage *>(child_page->GetData());
  // Change pagrent_page_id of child page
  btree_internal_child_page->SetParentPageId(id);
  buffer_pool_manager->UnpinPage(child_id, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager)
{
  int size = GetSize();
  std::pair<KeyType, ValueType> last = array[size - 1];
  recipient->CopyFirstFrom(last, parent_index, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager)
{
  int parent_page_id = GetParentPageId();
  int page_id = GetPageId();
  Page *parent_page = buffer_pool_manager->FetchPage(parent_page_id);
  BPlusTreeInternalPage *btree_internal_parent_page =
      reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());

  int size = GetSize();

  // Shoule valid the key of first index
  // KeyType key_of_first_index = btree_internal_parent_page->KeyAt(parent_index);
  // array[0].first = key_of_first_index;

  // Insert pair into first index of the page
  memmove(array + 1, array, size * sizeof(MappingType));
  array[0] = pair;
  IncreaseSize(1);

  // Update the key of parent_page
  btree_internal_parent_page->SetKeyAt(parent_index, pair.first);
  buffer_pool_manager->UnpinPage(parent_page_id, true);

  // Update the parent_page_id of child_page
  Page *child_page = buffer_pool_manager->FetchPage(pair.second);
  BPlusTreeInternalPage *btree_internal_child_page =
      reinterpret_cast<BPlusTreeInternalPage *>(child_page->GetData());
  page_id_t child_page_id = child_page->GetPageId();
  btree_internal_child_page->SetParentPageId(page_id);
  buffer_pool_manager->UnpinPage(child_page_id, true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager)
{
  for (int i = 0; i < GetSize(); i++)
  {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const
{
  if (GetSize() == 0)
  {
    return "";
  }
  std::ostringstream os;
  if (verbose)
  {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end)
  {
    if (first)
    {
      first = false;
    }
    else
    {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose)
    {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                     GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                     GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                     GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                     GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                     GenericComparator<64>>;
} // namespace cmudb
