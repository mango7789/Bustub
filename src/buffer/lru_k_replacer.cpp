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

#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

LRUKReplacer::~LRUKReplacer() {
  for (auto iter : node_store_) {
    LRUKNode* node = iter.second;
    delete node;
  }
}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  size_t max_time = 0;
  size_t least_time = SIZE_MAX;
  LRUKNode* curr_node = nullptr;
  LRUKNode* evict_node = nullptr;

  for (auto iter : node_store_) {
    curr_node = iter.second;
    if (!curr_node->is_evictable_) continue;

    if (curr_node->history_.size() < k_) {
      if (max_time == SIZE_MAX) {
        size_t lst_time = curr_node->history_.front();
        if (lst_time < least_time) {
          evict_node = curr_node;
          least_time = lst_time;
        }
      }
      else {
        max_time = SIZE_MAX;
        evict_node = curr_node;
        least_time = curr_node->history_.front();
      }
    }
    else {
      if (max_time != SIZE_MAX) {
        size_t max_diff = current_timestamp_ - curr_node->history_.front();
        if (max_diff > max_time) {
          max_time = max_diff;
          evict_node = curr_node;
        }
      }
    }
  }

  std::cout << std::endl;
  
  if (evict_node != nullptr) {
    frame_id_t fid = evict_node->fid_;
    LRUKReplacer::Remove(fid);
    return std::optional(fid);
  } else {
    return std::nullopt;
  }

}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  auto node_iter = node_store_.find(frame_id);
  latch_.lock();
  current_timestamp_++;
  latch_.unlock();
  // Don't find the desired node
  if (node_iter == node_store_.end()) {
    // The node store is already full
    BUSTUB_ASSERT(node_store_.size() != replacer_size_, "The frame id is invalid!");
    // Create a new LRUK Node
    LRUKNode* new_node = new LRUKNode();
    new_node->history_.push_back(current_timestamp_);
    new_node->fid_ = frame_id;
    latch_.lock();
    node_store_[frame_id] = new_node;
    latch_.unlock();
  }
  else {
    LRUKNode* curr_node = node_iter->second;
    latch_.lock();
    if (curr_node->history_.size() == k_) {
      curr_node->history_.pop_front();
    }
    curr_node->history_.push_back(current_timestamp_);
    latch_.unlock();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  auto node_iter = node_store_.find(frame_id);
  if (node_iter == node_store_.end()) {
    return;
  }

  latch_.lock();
  LRUKNode* node = node_iter->second;
  if (set_evictable) {
    if (!node->is_evictable_) {
      curr_size_++;
      node->is_evictable_ = true;
    }
  } else {
    if (node->is_evictable_) {
      curr_size_--;
      node->is_evictable_ = false;
    }
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto node_iter = node_store_.find(frame_id);
  // Don't find the desired frame
  if (node_iter == node_store_.end()) {
    return;
  }
  LRUKNode* curr_node = node_iter->second;
  if (!curr_node->is_evictable_) {
    throw std::runtime_error("The frame " + std::to_string(frame_id) + " is not evictable!");
  }

  latch_.lock();
  node_store_.erase(frame_id);
  delete curr_node;
  curr_size_--;
  latch_.unlock();

  return;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
