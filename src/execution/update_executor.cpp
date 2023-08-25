//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple to_update_tuple{};
  TupleMeta to_update_tuple_meta{};
  to_update_tuple_meta.is_deleted_ = true;
  RID emit_rid;
  int32_t update_count = 0;
  while (child_executor_->Next(&to_update_tuple, &emit_rid)) {
    table_info_->table_->UpdateTupleMeta(to_update_tuple_meta, emit_rid);
    for (auto index : table_indexes_) {
      index->index_->DeleteEntry(to_update_tuple, emit_rid, exec_ctx_->GetTransaction());
    }
    to_update_tuple_meta.is_deleted_ = false;
    table_info_->table_->InsertTuple(to_update_tuple_meta, to_update_tuple);
    for (auto index : table_indexes_) {
      index->index_->InsertEntry(to_update_tuple, emit_rid, exec_ctx_->GetTransaction());
    }
    ++update_count;
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, update_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
