//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::scoped_lock buffer_latch{latch_};
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);

    // we should lock the page's write latch and add count
    pages_[frame_id].pin_count_++;
    latch_.unlock();
    return &pages_[frame_id];
  }

  frame_id_t frame_id = PickVictimFrameL();
  if (frame_id < 0) {
    latch_.unlock();
    return nullptr;
  }
  page_id_t victim_page_id = pages_[frame_id].GetPageId();  // page_table_[frame_id];
  if (pages_[frame_id].IsDirty()) {
    FlushPageImplL(page_table_[frame_id]);
  }
  page_table_.erase(victim_page_id);
  page_table_[page_id] = frame_id;

  UpdatePageMetaData(&pages_[frame_id], page_id);
  pages_[frame_id].pin_count_ += 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  return &pages_[frame_id];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  // what if the page not in the page table, should we fetch it?
  // if pinned, the page should be in pages
  // unpin will the put the page into the replacer.

  std::scoped_lock buffer_latch{latch_};
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }

  if (pages_[frame_id].GetPinCount() <= 0) {
    latch_.unlock();
    return false;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
  }

  return true;
}

// flush the dirty page to the disk and reset the page dirty bit
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::scoped_lock buffer_latch{latch_};
  // Make sure you call DiskManager::WritePage!
  bool flush_success = FlushPageImplL(page_id);
  return flush_success;
}

bool BufferPoolManager::FlushPageImplL(page_id_t page_id) {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }

  return true;

}


Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  std::scoped_lock buffer_latch{latch_};
  if (AllPagesPinnedL()) {
    latch_.unlock();
    return nullptr;
  }

  page_id_t pid = disk_manager_->AllocatePage();
  frame_id_t frame_id = PickVictimFrameL();
  page_id_t victim_page_id = pages_[frame_id].GetPageId();

  // do we need to flush the victim page?
  if (pages_[frame_id].IsDirty()) {
    FlushPageImplL(victim_page_id);
  }
  // only after we flush the page we can erase the page_id from
  // the page_table, or the flush_page_impl won't flush
  // fuck.
  page_table_.erase(victim_page_id);

  UpdatePageMetaData(&pages_[frame_id], pid);
  pages_[frame_id].pin_count_ += 1;
  pages_[frame_id].ResetMemory();

  // erase the original page id from the page table.
  page_table_[pid] = frame_id;
  *page_id = pid;
  return &pages_[frame_id];

  // Page *page = FetchPageImpl(pid);
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::scoped_lock buffer_latch{latch_};
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }
  disk_manager_->DeallocatePage(page_id);
  page_table_.erase(page_id);
  UpdatePageMetaData(&pages_[frame_id], INVALID_PAGE_ID);
  free_list_.push_back(frame_id);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].IsDirty()) {
      FlushPageImplL(i);
    }
  }
}

frame_id_t BufferPoolManager::PickVictimFrameL() {
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&frame_id)) {
      // LOG_INFO("fetchpage: no victim is found");
    }
  }

  return frame_id;
}

bool BufferPoolManager::AllPagesPinnedL() { return replacer_->Size() == 0 && free_list_.empty(); }

}  // namespace bustub
