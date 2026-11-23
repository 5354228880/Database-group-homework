//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <functional>
#include <list>
#include <stdexcept>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  if (bucket_size == 0) {
    throw std::invalid_argument("bucket size must be positive");
  }
  // 全局深度为 0 时，所有键都映射到目录中唯一的桶
  dir_.push_back(std::make_shared<Bucket>(bucket_size_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  return std::hash<K>{}(key) & (dir_.size() - 1);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

// ==================== Added implementation: Find / Remove ====================
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto bucket = dir_[IndexOf(key)];
  while (!bucket->Insert(key, value)) {
    RedistributeBucket(bucket);
    // 多个键可能共享连续几个哈希低位，每次分裂后都要重新定位目标桶
    bucket = dir_[IndexOf(key)];
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  const int old_depth = bucket->GetDepth();
  const size_t split_bit = size_t{1} << old_depth;
  auto sibling = std::make_shared<Bucket>(bucket_size_, old_depth + 1);
  if (old_depth == global_depth_) {
    // 目录翻倍时先复制桶指针，让上下两半的对应目录项指向同一个桶
    const size_t old_size = dir_.size();
    dir_.resize(2 * old_size);
    std::copy_n(dir_.begin(), old_size, dir_.begin() + old_size);
    ++global_depth_;
  }

  // 桶内各键的哈希值具有相同的低 old_depth 位，新增位为 1 的目录项改指向新桶
  auto &items = bucket->GetItems();
  const size_t prefix = IndexOf(items.front().first) & (split_bit - 1);
  for (size_t index = prefix | split_bit; index < dir_.size(); index += 2 * split_bit) {
    dir_[index] = sibling;
  }
  auto &sibling_items = sibling->GetItems();
  for (auto it = items.begin(); it != items.end();) {
    auto item = it++;
    if ((std::hash<K>{}(item->first) & split_bit) != 0) {
      // splice 直接移动链表节点，避免复制键和值
      sibling_items.splice(sibling_items.end(), items, item);
    }
  }
  bucket->IncrementDepth();
  ++num_buckets_;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
// 新桶的链表初始为空，容量和局部深度由调用方指定
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // 只有找到对应的键才写入 value，查找失败时保留调用方传入的值
  for (const auto &[stored_key, stored_value] : list_) {
    if (stored_key == key) {
      value = stored_value;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // 每个键在桶中只占一个节点，删除匹配的节点后即可返回
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // 先查找已有键，即使桶已满，更新值也不占用新的位置
  for (auto &[stored_key, stored_value] : list_) {
    if (stored_key == key) {
      stored_value = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
