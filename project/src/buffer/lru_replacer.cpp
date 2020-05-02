/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"
#include <iostream>

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  auto iter = map_.find(value);

  list_.push_front(value);
  // if value has existed
  if (iter != map_.end()) {
    list_.erase(iter->second);
  } 
  map_[value] = list_.begin();
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  if (list_.empty()) {
    return false;
  }

  auto iter = list_.end();
  value = *(--iter);
  list_.pop_back();
  map_.erase(value);
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  auto iter = map_.find(value);
  if (iter == map_.end()) {
    return false;
  }
  list_.erase(iter->second);
  map_.erase(iter);
  return true;

}

template <typename T> size_t LRUReplacer<T>::Size() { return list_.size(); }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
