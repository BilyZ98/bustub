//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <assert.h>
#include <algorithm>
#include <mutex>
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  max_num_pages_ = num_pages;
  // ring_buffer_.resize(max_num_pages_);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lru_latch{latch_};
  // find one victim and evict it if there is frame exists
  if (ring_buffer_.empty()) {
    return false;
  }

  frame_id_t last_frame = ring_buffer_.back();
  ring_buffer_.pop_back();
  frame_it_map_.erase(last_frame);
  *frame_id = last_frame;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lru_latch{latch_};
  // remove the frame_id from lru
  if (frame_it_map_.find(frame_id) != frame_it_map_.end()) {
    std::list<frame_id_t>::iterator iter = frame_it_map_[frame_id];
    // frame_it_map_[frame_id] = ring_buffer_.end();
    frame_it_map_.erase(frame_id);
    ring_buffer_.erase(iter);

    assert(ring_buffer_.size() == frame_it_map_.size());
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lru_latch{latch_};

  // add the frame_id to lru
  if (frame_it_map_.find(frame_id) != frame_it_map_.end()) {
    return;
  }
  if (ring_buffer_.size() == max_num_pages_) {
    // frame_id_t *fid = nullptr;
    frame_id_t fid;
    Victim(&fid);
  }
  ring_buffer_.push_front(frame_id);
  frame_it_map_[frame_id] = ring_buffer_.begin();
}

size_t LRUReplacer::Size() { return ring_buffer_.size(); }

}  // namespace bustub
