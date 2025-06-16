#pragma once

#include <tbb/concurrent_unordered_map.h>

#include <memory>
#include <shared_mutex>
#include <utility>

template <typename U, typename T>
class RequestMap {
 public:
  using MapType = tbb::concurrent_unordered_map<U, std::shared_ptr<T>>;

  bool Insert(U req_id, std::shared_ptr<T>&& req) {
    auto [it, inserted] = map_.insert({req_id, std::move(req)});
    return inserted;
  }

  std::shared_ptr<T> Get(U req_id) const {
    auto it = map_.find(req_id);
    return (it != map_.end()) ? it->second : nullptr;
  }

  template <typename Fn>
  bool Modify(U req_id, Fn&& fn) {
    auto it = map_.find(req_id);
    if (it != map_.end()) {
      fn(it->second);
      return true;
    }
    return false;
  }

  bool Erase(U req_id) noexcept {
    std::unique_lock<std::shared_mutex> lock(map_mutex_);
    return map_.unsafe_erase(req_id) > 0;
  }

  void Clear() noexcept { map_.clear(); }

  size_t Size() const noexcept { return map_.size(); }

 private:
  std::shared_mutex map_mutex_;
  MapType map_;
};

template <typename K, typename V>
struct RequestCleaner {
  RequestCleaner(RequestMap<K, V>& map, const K& id)
      : map_(map), req_id_(id), active_(true) {}

  void Cancel() { active_ = false; }

  ~RequestCleaner() {
    if (active_) map_.Erase(req_id_);
  }

 private:
  RequestMap<K, V>& map_;
  K req_id_;
  bool active_;
};

template <typename K, typename V>
RequestCleaner(RequestMap<K, V>&, K) -> RequestCleaner<K, V>;
