## Physical Plan (ie Execution Engine)

## Why we need a Physical Plan? Can't we just use the Logical Plan?
A single logical plan can be executed in multiple ways. For example, for AggFunctions,
we can use a HashAgg or a OrderedAgg. The Logical Plan is just a description of what we want to do.
The Physical Plan is a description of how we want to do it.

```go
			// NOTE: this is a place where a single logical plan can be mapped to multiple physical plans based on
			// the strategy involved. Kind of like a strategy pattern.
			if shouldPlanOrderedAgg(lPlan.GroupExpr) {
				agg = operators.NewOrderedAggregate(groupByExpr, aggExpr)
			} else {
				agg = operators.NewHashAggregate(groupByExpr, aggExpr)
			}
```
