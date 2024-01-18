package physicalplan

import (
	"errors"
	logicalplan "tiny_dataframe/pkg/c_logical_plan"
	eval_expr "tiny_dataframe/pkg/d_physicalplan/a_eval_expr"
	operators "tiny_dataframe/pkg/d_physicalplan/b_operators"
	containers "tiny_dataframe/pkg/g_containers"
)

type QueryPlanner interface {
	CreatePhyExpr(e logicalplan.Expr, schema containers.ISchema) (eval_expr.Expr, error)
	CreatePhyPlan(lp logicalplan.LogicalPlan, state ExecState) (operators.PhysicalPlan, error)
}

type DefaultQueryPlanner struct {
}

func (d DefaultQueryPlanner) CreatePhyExpr(e logicalplan.Expr, schema containers.ISchema) (eval_expr.Expr, error) {
	switch v := e.(type) {
	case logicalplan.ColumnExpr:
		return eval_expr.ColumnExpr{Index: schema.IndexOf(v.Name)}, nil
	case logicalplan.LiteralInt64Expr:
		return eval_expr.LiteralInt64Expr{Value: v.Val}, nil
	case logicalplan.LiteralFloat64Expr:
		return eval_expr.LiteralFloat64Expr{Value: v.Val}, nil
	case logicalplan.LiteralStringExpr:
		return eval_expr.LiteralStringExpr{Value: v.Val}, nil
	case logicalplan.BooleanBinaryExpr:
		l, err := d.CreatePhyExpr(v.L, schema)
		if err != nil {
			return nil, err
		}
		r, err := d.CreatePhyExpr(v.R, schema)
		if err != nil {
			return nil, err
		}
		return eval_expr.BooleanBinaryExpr{L: l, R: r, Op: v.Op}, nil
	case logicalplan.AggregateExpr:
		// TODO: not being used. make it work.
		inner, err := d.CreatePhyExpr(v.Expr, schema)
		if err != nil {
			return nil, err
		}
		return eval_expr.AggregateExpr{Name: v.Name, Expr: inner}, nil
	default:
		return nil, errors.New("expr not implemented")
	}
}

func (d DefaultQueryPlanner) CreatePhyPlan(lp logicalplan.LogicalPlan, state ExecState) (operators.PhysicalPlan, error) {
	var visitErr error
	var source operators.PhysicalPlan
	var prev operators.PhysicalPlan
	lp.Accept(PostPlanVisitorFunc(func(plan logicalplan.LogicalPlan) bool {
		switch lPlan := plan.(type) {
		case logicalplan.Input:
			scan := &operators.Input{Source: lPlan.Source, Projection: lPlan.Projection}
			source = scan
			prev = scan
		case logicalplan.Projection:
			projExpr := make([]eval_expr.Expr, len(lPlan.Proj))
			for i, e := range lPlan.Proj {
				schema := prev.Schema()
				projExpr[i], _ = d.CreatePhyExpr(e, schema)
			}
			projSchema := lPlan.Schema()

			projection := &operators.Projection{Proj: projExpr, Sch: projSchema}
			prev.SetNext(projection)
			prev = projection

		case logicalplan.Selection:
			schema := prev.Schema()
			filterExpr, _ := d.CreatePhyExpr(lPlan.Filter, schema)

			selection := &operators.Selection{Sch: schema, Filter: filterExpr}
			prev.SetNext(selection)
			prev = selection
		case logicalplan.Aggregate:
			shouldPlanOrderedAgg := func(groupByList []logicalplan.Expr) bool {
				//TODO: this is a dummy rule
				if len(groupByList) > 1 {
					return true
				}
				return false
			}

			groupByExpr := make([]eval_expr.Expr, len(lPlan.GroupExpr))
			schema := prev.Schema()
			for i, e := range lPlan.GroupExpr {
				groupByExpr[i], _ = d.CreatePhyExpr(e, schema)
			}

			aggExpr := make([]eval_expr.AggregateExpr, len(lPlan.AggregateExpr))
			for i, e := range lPlan.AggregateExpr {
				inner, _ := d.CreatePhyExpr(e.Expr, schema)
				//TODO: modify this to use d.CreatePhyExpr(e.Expr, schema)
				aggExpr[i] = eval_expr.AggregateExpr{Name: e.Name, Expr: inner}
			}

			var agg operators.PhysicalPlan
			// NOTE: this is a place where a single logical plan can be mapped to multiple physical plans based on
			// the strategy involved. Kind of like a strategy pattern.
			if shouldPlanOrderedAgg(lPlan.GroupExpr) {
				agg = operators.NewOrderedAggregate(groupByExpr, aggExpr)
			} else {
				agg = operators.NewHashAggregate(groupByExpr, aggExpr)
			}
			prev.SetNext(agg)
			prev = agg

		case logicalplan.Output:
			callback := lPlan.Callback
			out := &operators.Output{OutputCallback: callback}
			prev.SetNext(out)
			prev = out
		default:
			visitErr = errors.New("plan not implemented")
		}
		return visitErr == nil
	}))

	return source, visitErr
}
