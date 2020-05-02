#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb
{

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) : bucket_size_(size), global_depth_(0){
  buckets_.resize(1<<(global_depth_+1));
  for (size_t i = 0; i < buckets_.size(); i++) {
    buckets_[i] = std::make_shared<Bucket<K, V>>(i, 0, bucket_size_);
  }
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key)
{
  return Hasher<K>()(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const
{
  return global_depth_;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const
{
  return buckets_[bucket_id]->GetLocalDepth();
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const
{
  return static_cast<int>(buckets_.size());
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value)
{
  std::lock_guard<std::mutex> guard(mutex_);
  size_t id = GETBIT(Hasher<K>()(key), global_depth_);
  return buckets_[id]->Get(key, value);
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key)
{
  std::lock_guard<std::mutex> guard(mutex_);
  int id = GETBIT(Hasher<K>()(key), global_depth_);
  return buckets_[id]->Remove(key);
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value)
{
  std::lock_guard<std::mutex> guard(mutex_);

  // get the key of k
  int id = GETBIT(Hasher<K>()(key), global_depth_);

  auto &bucket = buckets_[id];
  if (bucket.get() == nullptr) {
    buckets_[id] = std::make_shared<Bucket<K, V>>(id, 0, bucket_size_);
    buckets_[id]->Put(key, value);
  }
  else {
    // overflow
    if (!bucket->Put(key, value))
    {
      // Split bucket into two bucket, set id and ++local_depth_
      auto new_bucket = bucket->Split();
      int new_id = new_bucket->GetId();
      if (bucket->GetLocalDepth() > global_depth_) {
        ++global_depth_;
        expand(new_id, std::move(new_bucket));
      } else if (bucket->GetLocalDepth() <= global_depth_) {
        // DEBUG
        buckets_[new_id] = std::move(new_bucket);
      } else {
        // TODO: ERROR
        assert(1 == 0);
      }
    }
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
