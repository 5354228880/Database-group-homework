//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

// ===== 新增的私有辅助函数 =====

auto BufferPoolManagerInstance::AcquireFrame(frame_id_t *frame_id) -> bool {
  // 1) 优先复用 free list 中的空闲帧
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }

  // 2) 没有空闲帧时，用 LRU-K replacer 淘汰一个可淘汰的帧。
  //    若一个可淘汰帧都没有（缓冲池全被 pin 住），说明暂时没有空间可用。
  frame_id_t victim_frame;
  if (!replacer_->Evict(&victim_frame)) {
    return false;
  }

  // 3) 淘汰的帧上还缓存着旧页面：脏页要先写回磁盘，再解除映射并清空该帧
  Page *victim_page = &pages_[victim_frame];
  page_id_t victim_page_id = victim_page->GetPageId();
  if (victim_page->IsDirty()) {
    disk_manager_->WritePage(victim_page_id, victim_page->GetData());
  }
  page_table_->Remove(victim_page_id);
  ResetFrame(victim_page);

  *frame_id = victim_frame;
  return true;
}

void BufferPoolManagerInstance::ResetFrame(Page *page) {
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
}

// ===== 对外提供的页面操作 =====

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!AcquireFrame(&frame_id)) {
    // 缓冲池已满且所有页面都被 pin 住，无法再新建页面
    return nullptr;
  }

  // AcquireFrame 返回的帧已经是干净的空帧，这里只需要填写新页面的元数据
  Page *page = &pages_[frame_id];
  page_id_t new_page_id = AllocatePage();
  page->page_id_ = new_page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  page_table_->Insert(new_page_id, frame_id);

  // 记录这次访问，并在上层调用 UnpinPage 之前禁止淘汰这个帧
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  *page_id = new_page_id;
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  // 1) 页面已经在缓冲池中：直接返回，同时把 pin count 加一
  if (page_table_->Find(page_id, frame_id)) {
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

  // 2) 页面不在缓冲池中：取一个帧，把磁盘上的页面读进该帧
  if (!AcquireFrame(&frame_id)) {
    return nullptr;
  }
  Page *page = &pages_[frame_id];
  disk_manager_->ReadPage(page_id, page->GetData());
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}


auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;                              // 不在缓冲池里
  }

  Page *page = &pages_[frame_id];
  if (page->pin_count_ <= 0) {
    return false;                              // 已经没人 pin，再 unpin 是调用方的 bug
  }

  page->pin_count_--;
  if (is_dirty) {
    page->is_dirty_ = true;                    // 只置位，绝不清位
  }
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);   // 归零才允许淘汰
  }
  return true;
}


auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->GetData());   // 不管 dirty 都写
  page->is_dirty_ = false;
  return true;
}


void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = &pages_[i];
    if (page->page_id_ == INVALID_PAGE_ID) {
      continue;                                // 空闲帧，没有页面可刷
    }
    disk_manager_->WritePage(page->page_id_, page->GetData());
    page->is_dirty_ = false;
  }
}


auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;                               // 本来就不在，视为删除成功（幂等）
  }

  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    return false;                              // 还被引用，什么都不动
  }

  replacer_->Remove(frame_id);                 // 先撤淘汰资格
  page_table_->Remove(page_id);
  ResetFrame(page);                            // 清内存 + page_id/pin_count/dirty 归零
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
