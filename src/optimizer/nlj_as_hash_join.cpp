#include <algorithm>
#include <memory>
#include <utility>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "fmt/core.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    // Check if expr is equal condition where one is for the left table, and one is for the right table.
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      // t1.a = t2.a
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          std::vector<AbstractExpressionRef> left_exprs;
          std::vector<AbstractExpressionRef> right_exprs;
          for (auto const &expr : expr->children_) {
            if (dynamic_cast<ColumnValueExpression *>(expr.get())->GetTupleIdx() == 0) {
              left_exprs.emplace_back(expr);
            } else {
              right_exprs.emplace_back(expr);
            }
          }
          return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                    nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                    std::move(right_exprs), nlj_plan.GetJoinType());
        }
      }
    }
    if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      // t1.a = t2.a and t1.b = t2.b
      if (expr->logic_type_ == LogicType::And) {
        std::vector<AbstractExpressionRef> left_exprs;
        std::vector<AbstractExpressionRef> right_exprs;
        for (const auto &left_expr : expr->children_) {
          for (auto const &expr : left_expr->children_) {
            if (dynamic_cast<ColumnValueExpression *>(expr.get())->GetTupleIdx() == 0) {
              left_exprs.emplace_back(expr);
            } else {
              right_exprs.emplace_back(expr);
            }
          }
        }
        auto plan_temp =
            std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                               std::move(left_exprs), std::move(right_exprs), nlj_plan.GetJoinType());
        fmt::print("{}\n", plan_temp->ToString());
        // fmt::print("{}\n", plan_vec.size());
        return plan_temp;
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
