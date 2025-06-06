// migration_state_machine.hpp
#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <unordered_map>

#include "c_m_proto.h"

class MigrationStateMachine {
 public:
  enum class Mode : uint8_t { NONE = 0, SOURCE = 1, DESTINATION = 2 };

  enum class State : uint8_t {
    NO = MIGRATION_NO,
    PREPARING = MIGRATION_PREPARING,        // 仅源模式
    TRANSFERRING = MIGRATION_TRANSFERRING,  // 两种模式共有
    DONE = MIGRATION_DONE                   // 两种模式共有
  };

  struct SourceMigrationMeta {
    uint32_t id = 0;
    uint8_t dst_rack = 0;
    std::shared_ptr<std::vector<std::string>> keys;
  };

  struct DestinationMigrationMeta {
    uint32_t id = 0;
    uint8_t src_rack = 0;
    std::shared_ptr<std::vector<std::string>> keys;
    std::unordered_set<uint16_t> received_chunks;
    uint16_t total_chunks;
  };

  class Observer {
   public:
    virtual ~Observer() = default;
    virtual void onStateChange(State new_state, uint32_t migration_id) = 0;
    virtual void onModeChange(Mode new_mode) = 0;
  };

  explicit MigrationStateMachine(Observer* observer = nullptr)
      : observer_(observer) {
    buildSourceStateRing();
  }

  ~MigrationStateMachine() {
    if (!source_head_) return;
    SourceStateNode* node = source_head_->next;
    while (node != source_head_) {
      SourceStateNode* next = node->next;
      delete node;
      node = next;
    }
    delete source_head_;
  }

  bool transitionTo(State new_state, uint32_t migration_id = 0) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (current_mode_ == Mode::SOURCE) {
      if (!isValidSourceTransition(current_source_state_, new_state)) {
        return false;
      }

      current_source_state_ = new_state;
      if (observer_) {
        std::shared_ptr<const SourceMigrationMeta> meta;
        {
          std::lock_guard<std::mutex> meta_lock(meta_mutex_);
          meta = source_meta_;
        }
        lock.unlock();
        observer_->onStateChange(new_state, migration_id);
        lock.lock();
      }

      if (new_state == State::DONE) {
        resetSourceMode();
      }
      return true;
    } else if (current_mode_ == Mode::DESTINATION && migration_id > 0) {
      auto it = destination_metas_.find(migration_id);
      if (it == destination_metas_.end() ||
          !isValidDestinationTransition(it->second.state, new_state)) {
        return false;
      }

      // State old_state = it->second.state;
      it->second.state = new_state;
      if (observer_) {
        auto meta = it->second.meta;
        lock.unlock();
        observer_->onStateChange(new_state, migration_id);
        lock.lock();
      }

      if (new_state == State::DONE) {
        destination_metas_.erase(migration_id);

        if (destination_metas_.empty()) {
          current_mode_ = Mode::NONE;
          if (observer_) {
            lock.unlock();
            observer_->onModeChange(Mode::NONE);
          }
        }
      }
      return true;
    }
    return false;
  }

  bool startSourceMode(uint32_t migration_id, uint8_t dest_rack,
                       std::vector<std::string> keys) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (current_mode_ != Mode::NONE || !destination_metas_.empty()) {
      return false;
    }

    auto meta = std::make_shared<SourceMigrationMeta>();
    meta->id = migration_id;
    meta->dst_rack = dest_rack;
    meta->keys = std::make_shared<std::vector<std::string>>(std::move(keys));

    {
      std::lock_guard<std::mutex> meta_lock(meta_mutex_);
      source_meta_ = meta;
    }

    current_mode_ = Mode::SOURCE;
    current_source_state_ = State::PREPARING;

    if (observer_) {
      observer_->onModeChange(Mode::SOURCE);
      observer_->onStateChange(State::PREPARING, migration_id);
    }
    return true;
  }

  bool addDestinationMode(uint32_t migration_id, uint8_t source_rack,
                          std::vector<std::string> keys) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (current_mode_ == Mode::SOURCE) {
      return false;
    }

    auto meta = std::make_shared<DestinationMigrationMeta>();
    meta->id = migration_id;
    meta->src_rack = source_rack;
    meta->keys = std::make_shared<std::vector<std::string>>(std::move(keys));

    DestinationMeta dmeta;
    dmeta.meta = meta;
    dmeta.state = State::TRANSFERRING;

    destination_metas_[migration_id] = std::move(dmeta);

    Mode old_mode = current_mode_;
    current_mode_ = Mode::DESTINATION;

    if (observer_) {
      if (old_mode != Mode::DESTINATION) {
        observer_->onModeChange(Mode::DESTINATION);
      }
      observer_->onStateChange(State::TRANSFERRING, migration_id);
    }
    return true;
  }

  std::shared_ptr<const SourceMigrationMeta> getSourceMigrationMeta() const {
    std::lock_guard<std::mutex> lock(meta_mutex_);
    return source_meta_;
  }

  std::shared_ptr<DestinationMigrationMeta> getDestinationMigrationMeta(
      uint32_t migration_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = destination_metas_.find(migration_id);
    return (it != destination_metas_.end()) ? it->second.meta : nullptr;
  }

  State getSourceState() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_source_state_;
  }

  State getDestinationState(uint32_t migration_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = destination_metas_.find(migration_id);
    return (it != destination_metas_.end()) ? it->second.state : State::NO;
  }

  Mode getCurrentMode() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_mode_;
  }

  std::set<uint32_t> getActiveDestinationMigrations() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::set<uint32_t> ids;
    for (const auto& [id, meta] : destination_metas_) {
      ids.insert(id);
    }
    return ids;
  }

  std::string stateToString(State state) const {
    switch (state) {
      case State::NO:
        return "NO";
      case State::PREPARING:
        return "PREPARING";
      case State::TRANSFERRING:
        return "TRANSFERRING";
      case State::DONE:
        return "DONE";
      default:
        return "UNKNOWN";
    }
  }

  MigrationStateMachine(const MigrationStateMachine&) = delete;
  MigrationStateMachine& operator=(const MigrationStateMachine&) = delete;

 private:
  struct SourceStateNode {
    State state;
    SourceStateNode* next;
    SourceStateNode* prev;

    explicit SourceStateNode(State s)
        : state(s), next(nullptr), prev(nullptr) {}
  };

  struct DestinationMeta {
    std::shared_ptr<DestinationMigrationMeta> meta;
    State state = State::NO;
  };

  void buildSourceStateRing() {
    source_head_ = new SourceStateNode(State::NO);
    auto* preparing = new SourceStateNode(State::PREPARING);
    auto* transferring = new SourceStateNode(State::TRANSFERRING);
    auto* done = new SourceStateNode(State::DONE);

    source_head_->next = preparing;
    source_head_->prev = done;
    preparing->next = transferring;
    preparing->prev = source_head_;
    transferring->next = done;
    transferring->prev = preparing;
    done->next = source_head_;
    done->prev = transferring;

    current_source_state_ = State::NO;
  }

  bool isValidSourceTransition(State current, State next) const {
    return (current == State::NO && next == State::PREPARING) ||
           (current == State::PREPARING && next == State::TRANSFERRING) ||
           (current == State::TRANSFERRING && next == State::DONE) ||
           (current == State::DONE && next == State::NO);
  }

  bool isValidDestinationTransition(State current, State next) const {
    return (current == State::TRANSFERRING && next == State::DONE) ||
           (current == State::DONE && next == State::NO);
  }

  void resetSourceMode() {
    current_mode_ = Mode::NONE;
    current_source_state_ = State::NO;
    {
      std::lock_guard<std::mutex> meta_lock(meta_mutex_);
      source_meta_.reset();
    }
    if (observer_) {
      observer_->onModeChange(Mode::NONE);
      observer_->onStateChange(State::NO, 0);
    }
  }

  // SourceStateRing
  SourceStateNode* source_head_ = nullptr;
  State current_source_state_ = State::NO;

  // migration_id -> DestinationMeta{Mate, State}
  std::unordered_map<uint32_t, DestinationMeta> destination_metas_;

  Mode current_mode_ = Mode::NONE;

  mutable std::mutex mutex_;
  Observer* observer_ = nullptr;

  mutable std::mutex meta_mutex_;
  std::shared_ptr<SourceMigrationMeta> source_meta_;
};
