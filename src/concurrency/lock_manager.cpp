//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <type_traits>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  std::shared_ptr<LockRequestQueue> queue;
  if (txn->GetState() == TransactionState::ABORTED) {
    fmt::print("test1");
    throw Exception("aborted");
  }
  if (txn->GetState() == TransactionState::COMMITTED) {
    fmt::print("test2");
    throw Exception("committed");
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ: {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        break;
      }
      case IsolationLevel::READ_COMMITTED: {
        if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED) {
          std::lock_guard<std::mutex> lg(table_lock_map_latch_);
          if (table_lock_map_.count(oid) == 0) {
            table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
          }
          queue = table_lock_map_[oid];
        } else {
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
        break;
      }
      case IsolationLevel::READ_UNCOMMITTED: {
        if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        break;
      }
    }
  }
  if (txn->GetState() == TransactionState::GROWING) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED: {
        if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
          std::lock_guard<std::mutex> lg(table_lock_map_latch_);
          if (table_lock_map_.count(oid) == 0) {
            table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
          }
          queue = table_lock_map_[oid];
        } else {
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        }
        break;
      }
      default: {
        std::lock_guard<std::mutex> lg(table_lock_map_latch_);
        if (table_lock_map_.count(oid) == 0) {
          table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
        }
        queue = table_lock_map_[oid];
        break;
      }
    }
  }

  std::unique_lock<std::mutex> lock(queue->latch_);
  bool update = false;
  for (auto it = queue->request_queue_.begin(); it != queue->request_queue_.end(); ++it) {
    auto e = *it;
    if (e->txn_id_ == txn->GetTransactionId()) {
      if (e->lock_mode_ == lock_mode) {
        return true;
      }
      if (queue->upgrading_ != INVALID_TXN_ID) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!UpdateLocksCompatible(e->lock_mode_, lock_mode)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      switch (e->lock_mode_) {
        case LockMode::EXCLUSIVE:
          txn->GetExclusiveTableLockSet()->erase(oid);
          break;
        case LockMode::SHARED:
          txn->GetSharedTableLockSet()->erase(oid);
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          txn->GetIntentionExclusiveTableLockSet()->erase(oid);
          break;
        case LockMode::INTENTION_SHARED:
          txn->GetIntentionSharedTableLockSet()->erase(oid);
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
          break;
      }
      queue->request_queue_.erase(it);
      delete e;
      update = true;
      break;
    }
  }
  auto lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  queue->request_queue_.emplace_back(lock_request);
  if (update) {
    queue->upgrading_ = txn->GetTransactionId();
  }
  // 获取锁
  while (!GrantLock(queue.get(), lock_request)) {
    queue->cv_.wait(lock);
  }
  lock_request->granted_ = true;
  if (update) {
    queue->upgrading_ = INVALID_TXN_ID;
  }
  // book keeping
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  auto queue = table_lock_map_[oid];
  //检查行锁是否已释放
  for (auto e : queue->request_queue_) {
    if (!e->rid_.IsInvalid()) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    }
  }
  auto it = queue->request_queue_.begin();
  bool flag = false;
  auto lockquest = *it;
  for (; it != queue->request_queue_.end(); ++it) {
    lockquest = *it;
    if (lockquest->txn_id_ == txn->GetTransactionId() && lockquest->granted_) {
      flag = true;
      break;
    }
  }
  if (!flag) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ: {
      if (lockquest->lock_mode_ == LockMode::SHARED || lockquest->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    }
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::READ_UNCOMMITTED: {
      if (lockquest->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    }
  }

  std::unique_lock<std::mutex> lock(queue->latch_);
  auto lock_mode = lockquest->lock_mode_;
  queue->request_queue_.remove(lockquest);
  delete lockquest;
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }
  queue->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  std::shared_ptr<LockRequestQueue> queue;
  if (txn->GetState() == TransactionState::ABORTED) {
    throw Exception("aborted");
  }
  if (txn->GetState() == TransactionState::COMMITTED) {
    throw Exception("committed");
  }
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ: {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        break;
      }
      case IsolationLevel::READ_COMMITTED: {
        if (lock_mode == LockMode::SHARED) {
          std::lock_guard<std::mutex> lg(table_lock_map_latch_);
          if (table_lock_map_.count(oid) == 0) {
            table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
          }
          queue = table_lock_map_[oid];
        } else {
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
        break;
      }
      case IsolationLevel::READ_UNCOMMITTED: {
        if (lock_mode == LockMode::EXCLUSIVE) {
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        break;
      }
    }
  }
  if (txn->GetState() == TransactionState::GROWING) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED: {
        if (lock_mode == LockMode::EXCLUSIVE) {
          std::lock_guard<std::mutex> lg(table_lock_map_latch_);
          if (table_lock_map_.count(oid) == 0) {
            table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
          }
          queue = table_lock_map_[oid];
        } else {
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        }
        break;
      }
      default: {
        std::lock_guard<std::mutex> lg(table_lock_map_latch_);
        if (table_lock_map_.count(oid) == 0) {
          table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
        }
        queue = table_lock_map_[oid];
        break;
      }
    }
  }
  //检查是否有表锁
  bool tablelock = false;
  for (auto e : queue->request_queue_) {
    if (e->txn_id_ == txn->GetTransactionId() && e->rid_.IsInvalid()) {
      if (RowTableLocksCompatible(lock_mode, e->lock_mode_)) {
        tablelock = true;
        break;
      }
    }
  }
  if (!tablelock) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  //升级锁
  std::unique_lock<std::mutex> lock(queue->latch_);
  bool update = false;
  for (auto it = queue->request_queue_.begin(); it != queue->request_queue_.end(); ++it) {
    auto e = *it;
    if (e->txn_id_ == txn->GetTransactionId() && e->rid_ == rid) {
      if (e->lock_mode_ == lock_mode) {
        return true;
      }
      if (queue->upgrading_ != INVALID_TXN_ID) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!UpdateLocksCompatible(e->lock_mode_, lock_mode)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      switch (e->lock_mode_) {
        case LockMode::EXCLUSIVE:
          txn->GetExclusiveRowLockSet()->erase(oid);
          break;
        case LockMode::SHARED:
          txn->GetSharedRowLockSet()->erase(oid);
          break;
        default:
          break;
      }
      queue->request_queue_.erase(it);
      delete e;
      update = true;
      break;
    }
  }
  auto lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  queue->request_queue_.emplace_back(lock_request);
  if (update) {
    queue->upgrading_ = txn->GetTransactionId();
  }
  // 获取锁
  while (!GrantLock(queue.get(), lock_request)) {
    queue->cv_.wait(lock);
  }
  lock_request->granted_ = true;
  if (update) {
    queue->upgrading_ = INVALID_TXN_ID;
  }
  // book keeping
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}
auto LockManager::RowTableLocksCompatible(LockMode r_lockmode, LockMode t_lockmode) -> bool {
  switch (r_lockmode) {
    case LockMode::EXCLUSIVE: {
      return t_lockmode == LockMode::SHARED_INTENTION_EXCLUSIVE || t_lockmode == LockMode::EXCLUSIVE ||
             t_lockmode == LockMode::INTENTION_EXCLUSIVE;
    }
    case LockMode::SHARED: {
      return true;
    }
    default:
      return false;
  }
  return false;
}
//升级表锁是否兼容
auto LockManager::UpdateLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::INTENTION_SHARED: {
      return l2 == LockMode::INTENTION_SHARED;
    }
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED: {
      if (l2 == LockMode::SHARED_INTENTION_EXCLUSIVE || l2 == LockMode::EXCLUSIVE) {
        return true;
      }
      throw Exception("INCOMPATIBLE_UPGRADE");
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      if (l2 == LockMode::EXCLUSIVE) {
        return true;
      }
      throw Exception("INCOMPATIBLE_UPGRADE");
    }
    default:
      throw Exception("INCOMPATIBLE_UPGRADE");
  }
  return false;
}
auto LockManager::GrantLock(LockRequestQueue *lock_request_queue, LockRequest *lock_request) -> bool {
  if (lock_request->rid_.IsInvalid()) {
    /*查看当前锁请求是否与所有的已经 granted 的请求兼容*/
    if (!std::all_of(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                     [&lock_request, this](auto &it) {
                       return !it->granted_ || AreLocksCompatible(lock_request->lock_mode_, it->lock_mode_);
                     })) {
      return false;
    }
    // 如果队列中存在锁升级请求，若锁升级请求正为当前请求，则优先级最高。否则代表其他事务正在尝试锁升级，优先级高于当前请求。
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      return lock_request_queue->upgrading_ == lock_request->txn_id_;
    }
    // 是否为第一个 waiting 状态的请求
    auto it = lock_request_queue->request_queue_.begin();
    for (; it != lock_request_queue->request_queue_.end(); ++it) {
      auto e = *it;
      // if (!e->granted_) {
      //   if (e->txn_id_ == lock_request->txn_id_) {
      //     return true;
      //   }
      //   break;
      // }
      if (!e->granted_) {
        if (e == lock_request) {
          return true;
        }
        break;
      }
    }
    // 查看当前锁请求是否与前面其他 waiting 请求兼容
    for (; it != lock_request_queue->request_queue_.end() && *it != lock_request; ++it) {
      auto e = *it;
      if (!(e->granted_ || AreLocksCompatible(lock_request->lock_mode_, e->lock_mode_))) {
        return false;
      }
    }
  } else {
    { /*查看当前锁请求是否与所有的已经 granted 的请求兼容*/
      bool table_flag = false;
      for (auto e : lock_request_queue->request_queue_) {
        if (e->txn_id_ != lock_request->txn_id_) {
          continue;
        }
        if (e == lock_request) {
          break;
        }
        if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
          if (e->granted_ && e->rid_.IsInvalid() &&
              (e->lock_mode_ == LockMode::EXCLUSIVE || e->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
               e->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
            table_flag = true;
          }
        } else {
          if (e->granted_ && e->rid_.IsInvalid()) {
            table_flag = true;
          }
        }
      }
      if (!table_flag) {
        return false;
      }
    }

    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      return lock_request_queue->upgrading_ == lock_request->txn_id_;
    }
    auto it = lock_request_queue->request_queue_.begin();
    for (; it != lock_request_queue->request_queue_.end(); ++it) {
      auto e = *it;
      if (!e->granted_) {
        if (e == lock_request) {
          return true;
        }
        break;
      }
    }
    // 查看当前锁请求是否与前面其他 waiting 请求兼容
    // for (; it != lock_request_queue->request_queue_.end() && *it != lock_request; ++it) {
    //   auto e = *it;
    //   if (!(e->granted_ || AreLocksCompatible(lock_request->lock_mode_, e->lock_mode_))) {
    //     return false;
    //   }
    // }
  }

  return true;
}
// table lock
auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::INTENTION_SHARED: {
      if (l2 == LockMode::EXCLUSIVE) {
        throw Exception("");
      }
      return true;
    }
    case LockMode::INTENTION_EXCLUSIVE: {
      if (l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE) {
        return true;
      }
      throw Exception("");
    }
    case LockMode::SHARED: {
      if (l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED) {
        return true;
      }
      throw Exception("");
    }
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      if (l2 == LockMode::INTENTION_SHARED) {
        return true;
      }
      throw Exception("");
    }
    case LockMode::EXCLUSIVE: {
      throw Exception("");
    }
  }
  return true;
}
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
