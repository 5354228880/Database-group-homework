//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

#include <utility>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);

  // 没有任何可淘汰的帧：返回 false，且不要动 *frame_id
  if (curr_size_ == 0) {
    return false;
  }

  // 排序键 <bool, size_t>：
  //   first  = false → 访问次数 < k，k 距离为 +inf，必须优先淘汰
  //   first  = true  → 访问次数 >= k，k 距离有限，排在 +inf 组之后
  //   second = history_ 队首时间戳，越小说明 k 距离越大，越先淘汰
  bool found = false;
  frame_id_t victim = 0;
  std::pair<bool, size_t> best_key{false, 0};

  for (const auto &entry : node_store_) {
    const auto &node = entry.second;
    if (!node.is_evictable_) {
      continue;
    }
    // 队首：不足 k 次时是首次访问时间；否则是第 k 次倒数访问的时间
    std::pair<bool, size_t> key{node.history_.size() >= k_, node.history_.front()};
    if (!found || key < best_key) {
      found = true;
      best_key = key;
      victim = entry.first;
    }
  }

  if (!found) {
    return false;
  }

  // 删除整个结点 = 一并清掉访问历史，避免下次 RecordAccess 复用旧历史
  node_store_.erase(victim);
  --curr_size_;
  *frame_id = victim;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  BUSTUB_ASSERT(frame_id >= 0 && static_cast<size_t>(frame_id) < replacer_size_,
                "RecordAccess: frame id is out of range");

  auto &node = node_store_[frame_id];
  node.history_.push_back(current_timestamp_++);
  if (node.history_.size() > k_) {
    node.history_.pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);

  // 必须用 find：帧不存在时不能用 operator[] 偷偷建一个空结点
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  // 状态未变化时直接返回，保证 curr_size_ 不会被重复加减
  if (it->second.is_evictable_ == set_evictable) {
    return;
  }

  it->second.is_evictable_ = set_evictable;
  if (set_evictable) {
    ++curr_size_;
  } else {
    --curr_size_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  // 非 evictable 静默忽略：并发测试里 Remove 与 RecordAccess 会撞车，这里不能 abort
  if (!it->second.is_evictable_) {
    return;
  }

  node_store_.erase(it);
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return curr_size_;
}

}  // namespace bustub
