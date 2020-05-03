/*
 * buffer_pool_manager.h
 *
 * Functionality: The simplified Buffer Manager interface allows a client to
 * new/delete pages on disk, to read a disk page into the buffer pool and pin
 * it, also to unpin a page in the buffer pool.
 */

#pragma once
#include <list>
#include <mutex>

#include "buffer/lru_replacer.h"
#include "disk/disk_manager.h"
#include "hash/extendible_hash.h"
#include "logging/log_manager.h"
#include "page/page.h"

namespace cmudb {
class BufferPoolManager {
public:
  BufferPoolManager(size_t pool_size, DiskManager *disk_manager,
                          LogManager *log_manager = nullptr);

  ~BufferPoolManager();

  Page *FetchPage(page_id_t page_id);

  bool UnpinPage(page_id_t page_id, bool is_dirty);

  bool FlushPage(page_id_t page_id);

  Page *NewPage(page_id_t &page_id);

  bool DeletePage(page_id_t page_id);

private:
  void InitPageMetadata(page_id_t pid, Page* page) {
    page->page_id_ = pid;
    page->is_dirty_ = false;
    page->pin_count_ = 1;
  }

  void ResetPageMetadata(Page* page) {
    page->page_id_ = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    page->pin_count_ = 0;
  }

  void SetPageDirty(Page* page) {
    page->is_dirty_ = true;
  }

  void SetPageUnpin(Page* page) {
    --(page->pin_count_);
  }

  void SetPagePin(Page* page) {
    ++(page->pin_count_);
  }

private:
  size_t pool_size_; // number of pages in buffer pool
  Page *pages_;      // array of pages
  DiskManager *disk_manager_;
  LogManager *log_manager_;
  HashTable<page_id_t, Page *> *page_table_; // to keep track of pages
  Replacer<Page *> *replacer_;   // to find an unpinned page for replacement
  std::list<Page *> *free_list_; // to find a free page for replacement
  std::mutex latch_;             // to protect shared data structure
};
} // namespace cmudb
