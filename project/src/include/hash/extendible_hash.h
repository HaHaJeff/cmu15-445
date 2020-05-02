/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <memory>
#include <map>
#include <mutex>
#include <assert.h>
#include "hash/hash_table.h"

namespace cmudb {

// set specify bit to 1
#define SETBIT(x, y) (x | (1 << (y)))
// get lowest y bit
#define GETBIT(x, y) (x & ~(0xFFFFFFFF << (y+1)))
// get specify bit is zero
#define ISZERO(x, y) (!(x & (1 << (y))))

template <typename K>
struct Hasher {
  size_t operator()(const K& k) { return std::hash<K>()(k); }
};

template <typename K, typename V>
class Bucket {
public:
  explicit Bucket(int id, int depth, size_t size) : id_(id), local_depth_(depth), size_(size){
  }

  bool Put(const K& k, const V& v) {
    data_.insert(std::make_pair(k, v));  

    // overflow, return false;
    if (data_.size() == size_) {
      return false;
    }  

    return true;
  }

  bool Get(const K& k, V& v) {
    auto iter = data_.find(k);

    // not exists
    if (iter == data_.end()) {
      return false;
    }

    v = iter->second;
    return true;
  }

  bool Remove(const K& k) {
    auto iter = data_.find(k);
    if (iter == data_.end()) {
      return false;
    }
    data_.erase(iter);
    return true;
  }

  int GetLocalDepth() {
    return local_depth_;
  }

  void SetLocalDepth(int local_depth) {
    local_depth_ = local_depth;
  }

  /*
   * split when bucket is full;
   */
  std::unique_ptr<Bucket> Split() {
    local_depth_++;
    auto new_bucket = std::make_unique<Bucket>(SETBIT(id_, local_depth_), local_depth_, size_);

    for (typename std::map<K, V>::iterator iter = data_.begin(); iter != data_.end();) {
        if (!ISZERO(Hasher<K>()(iter->first), local_depth_)) {
            new_bucket->Put(iter->first, iter->second);
            data_.erase(iter++);
        } else {
          iter++;
        } 
    }
    return new_bucket;
  }

  int GetId() const {
    return id_;
  }

  std::map<K, V> GetData() const {
    return data_;
  }
private:
  // index in directoru
  int id_;
  int local_depth_;
  // bucket size
  size_t size_;
  std::map<K, V> data_;
};
template <typename K, typename V>
using BucketPtr = std::shared_ptr<Bucket<K, V>>;

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

private:
  void expand(int id, std::unique_ptr<Bucket<K, V>> bucket) {
    // global_depth_ init value is 0
    buckets_.resize(1 << (global_depth_+1));
    buckets_[id] = std::move(bucket);    
    for (int i = 0; i < (1 << global_depth_); i++) {
      if (buckets_[i].get() == nullptr || buckets_[i]->GetLocalDepth() < global_depth_) {
        buckets_[SETBIT(i, global_depth_)] = buckets_[i];
      }
    }
  }

private:
  // add your own member variables here
  int bucket_size_;
  int global_depth_;
  std::mutex mutex_;
  std::vector<BucketPtr<K, V>> buckets_;
};
} // namespace cmudb
