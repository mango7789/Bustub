#include "primer/hyperloglog_presto.h"

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0), leading_b(n_leading_bits) {
  dense_bucket_ = std::vector<std::bitset<DENSE_BUCKET_SIZE>>(std::pow(2, leading_b));
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hash_value = CalculateHash(val);
  std::bitset<64> bset = std::bitset<64>(hash_value);
  // Calculate the address
  uint16_t j = 0;
  for (int i = 64 - 1; i >= 64 - leading_b; i--) {
    if (bset.test(i)) {
      j += (1U << (i + leading_b - 64));
    }
  }
  // Calculate the value of p
  uint64_t p = 0;
  for (int i = 0; i < 64 - leading_b; i++) {
    if (bset.test(i)) {
      break;
    }
    p++;
  }
  p &= 0x7F;  // Get the lower 7 bit

  // Fetch the current value in the bucket
  uint16_t curr_value = dense_bucket_[j].to_ullong();
  if (overflow_bucket_.find(j) != overflow_bucket_.end()) {
    curr_value += overflow_bucket_[j].to_ullong() << 4;
  }
  // Update the value in the bucket
  if (p > curr_value) {
    dense_bucket_[j] = std::bitset<DENSE_BUCKET_SIZE>(p);
    if (p >> 4) {
      overflow_bucket_[j] = std::bitset<OVERFLOW_BUCKET_SIZE>(p >> 4);
    }
  }
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double enumer = 0.0;
  size_t m = dense_bucket_.size();
  for (size_t i = 0; i < m; i++) {
    uint64_t reg_value = dense_bucket_[i].to_ullong();
    if (overflow_bucket_.find(i) != overflow_bucket_.end()) {
      reg_value += overflow_bucket_[i].to_ullong() << 4;
    }
    enumer += 1 / std::pow(2, reg_value);
  }
  cardinality_ = enumer != 0 ? static_cast<size_t>(CONSTANT * m * m / enumer) : 0;
  return;
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
