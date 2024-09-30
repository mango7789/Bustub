#include "primer/hyperloglog.h"

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0), b(n_bits) {
  registers = std::vector<uint64_t>(std::pow(2, b), 0);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  return std::bitset<BITSET_CAPACITY>(hash);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  uint64_t pos = 0;
  for (int j = BITSET_CAPACITY - 1 - b; j >= 0; j--) {
    if (bset.test(j)) {
      return ++pos;
    }
    pos++;
  }
  
  return 0;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  // Calculate the hash value of the val
  hash_t hash_value = CalculateHash(val);
  // Conver to bitset
  std::bitset<BITSET_CAPACITY> bset = ComputeBinary(hash_value);
  // Get the value of j and p
  uint64_t j = 0;
  for (int i = BITSET_CAPACITY - 1; i >= BITSET_CAPACITY - b; i--) {
    if (bset.test(i)) {
      j += (1ULL << (i + b - BITSET_CAPACITY));
    }
  }
  uint64_t p = PositionOfLeftmostOne(bset);
  // Update the registers
  registers[j] = std::max(registers[j], p);
  return; 
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double enumer = 0.0;
  size_t m = registers.size();
  for (size_t j = 0; j < m; j++) {
    enumer += 1 / std::pow(2, registers[j]);
  }
  cardinality_ = enumer != 0 ? static_cast<size_t>(CONSTANT * m * m / enumer) : 0;
  return;
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
