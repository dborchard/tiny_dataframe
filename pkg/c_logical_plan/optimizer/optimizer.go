package rbo

import (
	"fmt"
	"tiny_dataframe/pkg/c_logical_plan"
)

type Optimizer struct {
	rules []Rule
}

func NewOptimizer() *Optimizer {
	return &Optimizer{
		rules: []Rule{
			ProjectionPushDownRule{},
		},
	}
}

func (o Optimizer) Optimize(plan logicalplan.LogicalPlan) logicalplan.LogicalPlan {
	iterPlan := plan
	for _, rule := range o.rules {
		iterPlan = rule.optimize(iterPlan)
	}
	return iterPlan
}

type Rule interface {
	optimize(plan logicalplan.LogicalPlan) logicalplan.LogicalPlan
}

type ProjectionPushDownRule struct{}

func (p ProjectionPushDownRule) optimize(plan logicalplan.LogicalPlan) logicalplan.LogicalPlan {
	accCols := make([]string, 0)
	return p.pushDown(plan, &accCols)
}

func (p ProjectionPushDownRule) pushDown(plan logicalplan.LogicalPlan, accCols *[]string) logicalplan.LogicalPlan {
	switch castPlan := plan.(type) {
	case logicalplan.Output:
		input := p.pushDown(castPlan.Next, accCols)
		return &logicalplan.Output{Next: input, Callback: castPlan.Callback}
	case logicalplan.Projection:
		p.extractColsForAllExpr(castPlan.Proj, castPlan.Next, accCols)
		input := p.pushDown(castPlan.Next, accCols)
		return &logicalplan.Projection{Next: input, Proj: castPlan.Proj}
	case logicalplan.Selection:
		p.extractCols(castPlan.Filter, castPlan.Next, accCols)
		input := p.pushDown(castPlan.Next, accCols)
		return &logicalplan.Selection{Next: input, Filter: castPlan.Filter}
	case logicalplan.Aggregate:
		p.extractColsForAllExpr(castPlan.GroupExpr, castPlan.Next, accCols)
		aggExprList := make([]logicalplan.Expr, 0)
		for _, ae := range castPlan.AggregateExpr {
			aggExprList = append(aggExprList, ae.Expr)
		}
		p.extractColsForAllExpr(aggExprList, castPlan.Next, accCols)
		input := p.pushDown(castPlan.Next, accCols)
		return &logicalplan.Aggregate{Next: input, GroupExpr: castPlan.GroupExpr, AggregateExpr: castPlan.AggregateExpr}
	case logicalplan.Input:
		// the bottom most logical plan
		return &logicalplan.Input{Path: castPlan.Path, Source: castPlan.Source, Projection: p.distinctCols(*accCols)}
	default:
		panic(fmt.Sprintf("ProjectionPushDownRule not support plan: %s", castPlan))
	}
}

func (p ProjectionPushDownRule) extractColsForAllExpr(exprList []logicalplan.Expr, input logicalplan.LogicalPlan, accCols *[]string) {
	for _, e := range exprList {
		p.extractCols(e, input, accCols)
	}
}

func (p ProjectionPushDownRule) extractCols(expr logicalplan.Expr, input logicalplan.LogicalPlan, accCols *[]string) {
	for _, col := range expr.ColumnsUsed(input) {
		*accCols = append(*accCols, col.Name)
	}
}

func (p ProjectionPushDownRule) distinctCols(accCols []string) []string {
	colSet := make(map[string]struct{})
	newCols := make([]string, 0)
	for _, col := range accCols {
		if _, ok := colSet[col]; !ok {
			colSet[col] = struct{}{}
			newCols = append(newCols, col)
		}
	}
	return newCols
}
