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
#include <vector>

#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"

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

  RID emit_rid;
  int32_t update_count = 0;
  while (child_executor_->Next(&to_update_tuple, &emit_rid)) {
    auto to_delete_tuple_meta = table_info_->table_->GetTupleMeta(emit_rid);
    to_delete_tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(to_delete_tuple_meta, emit_rid);
    for (auto index : table_indexes_) {
      index->index_->DeleteEntry(
          to_update_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
          emit_rid, exec_ctx_->GetTransaction());
    }
    /*获取要插入的Tuple*/
    std::vector<Value> to_insert{};
    to_insert.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    to_delete_tuple_meta.is_deleted_ = false;
    for (const auto &expr : plan_->target_expressions_) {
      to_insert.push_back(expr->Evaluate(&to_update_tuple, child_executor_->GetOutputSchema()));
    }
    to_update_tuple = Tuple(to_insert, &child_executor_->GetOutputSchema());
    auto rid_tmp = table_info_->table_->InsertTuple(to_delete_tuple_meta, to_update_tuple);
    if (rid_tmp.has_value()) {
      for (auto index : table_indexes_) {
        index->index_->InsertEntry(
            to_update_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
            rid_tmp.value(), exec_ctx_->GetTransaction());
      }
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
