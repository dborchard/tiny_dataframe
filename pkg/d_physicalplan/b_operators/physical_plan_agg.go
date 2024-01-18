package operators

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	eval_expr "tiny_dataframe/pkg/d_physicalplan/a_eval_expr"
	"tiny_dataframe/pkg/d_physicalplan/c_table_provider"
	execution "tiny_dataframe/pkg/e_exec_runtime"
	containers "tiny_dataframe/pkg/g_containers"
)

type HashAggregate struct {
	Next PhysicalPlan

	GroupByList []eval_expr.Expr
	AggExprList []eval_expr.AggregateExpr

	groupedData map[string]hashTuple
}

type hashTuple struct {
	groupedRow   []any
	accumulators []eval_expr.Accumulator
}

func NewHashAggregate(groupBy []eval_expr.Expr, aggExpr []eval_expr.AggregateExpr) PhysicalPlan {
	agg := HashAggregate{
		GroupByList: groupBy,
		AggExprList: aggExpr,
		groupedData: make(map[string]hashTuple),
	}
	return &agg
}

func (a *HashAggregate) Schema() containers.ISchema {
	// TODO: need to make it more generic
	// TODO: right now, the order is fixed.
	computeSchema := func() []arrow.Field {
		var fields []arrow.Field
		for _, expr := range a.GroupByList {
			fields = append(fields, arrow.Field{Name: expr.String(), Type: arrow.PrimitiveTypes.Int64})
		}
		for _, expr := range a.AggExprList {
			fields = append(fields, arrow.Field{Name: expr.String(), Type: arrow.PrimitiveTypes.Int64})
		}
		return fields
	}
	return containers.NewSchema(computeSchema(), nil)

}

func (a *HashAggregate) Children() []PhysicalPlan {
	return []PhysicalPlan{a.Next}
}

func (a *HashAggregate) Callback(ctx context.Context, batch containers.IBatch) error {
	currGroupByCols := make([]containers.IVector, len(a.GroupByList))
	for i, expr := range a.GroupByList {
		currGroupByCols[i], _ = expr.Evaluate(batch)
	}

	currAggExprCols := make([]containers.IVector, len(a.AggExprList))
	for i, expr := range a.AggExprList {
		currAggExprCols[i], _ = expr.Expr.Evaluate(batch)
	}

	for rowIdx := 0; rowIdx < batch.RowCount(); rowIdx++ {
		// 1. <col1, col2, col3>
		groupedRow := make([]any, len(currGroupByCols))
		for i, col := range currGroupByCols {
			groupedRow[i] = col.GetValue(rowIdx)
		}

		// 2. <col1, col2, col3> -> hash
		rowHash := a.encodeCols(groupedRow)

		// 3. <col1, col2, col3> -> hash -> <col1, col2, col3>
		//								 -> accumulators
		if _, ok := a.groupedData[rowHash]; !ok {
			a.groupedData[rowHash] = hashTuple{
				groupedRow:   groupedRow,
				accumulators: make([]eval_expr.Accumulator, len(a.AggExprList)),
			}
			for i, expr := range a.AggExprList {
				a.groupedData[rowHash].accumulators[i] = expr.CreateAccumulator()
			}
		}

		// 4. <col1, col2, col3> -> hash -> <col1, col2, col3>
		//								 -> accumulators -> accumulate()
		for i, _ := range a.AggExprList {
			value := currAggExprCols[i].GetValue(rowIdx)
			a.groupedData[rowHash].accumulators[i].Accumulate(value)
		}
	}

	return nil
}

func (a *HashAggregate) SetNext(next PhysicalPlan) {
	a.Next = next
}

func (a *HashAggregate) Execute(ctx *execution.TaskContext, callback tableprovider.Callback) error {
	panic("bug")
}

func (a *HashAggregate) Finish(ctx context.Context) error {
	res := make([]containers.IVector, 0)

	// 0. compute schema
	aggSchema := a.Schema()

	// 1. Add group by columns
	columnList := make([][]any, len(a.GroupByList))
	for i, _ := range columnList {
		columnList[i] = make([]any, 0)
	}
	for _, tuple := range a.groupedData {
		for colIdx, colVal := range tuple.groupedRow {
			columnList[colIdx] = append(columnList[colIdx], colVal)
		}
	}
	for _, col := range columnList {
		res = append(res, containers.NewVector(arrow.PrimitiveTypes.Int64, col))
	}

	// 2. Add aggregate columns
	for i, _ := range a.AggExprList {
		aggCol := make([]any, 0)
		for _, groupByRow := range a.groupedData {
			aggCol = append(aggCol, groupByRow.accumulators[i].FinalValue())
		}
		res = append(res, containers.NewVector(arrow.PrimitiveTypes.Int64, aggCol))
	}

	err := a.Next.Callback(ctx, containers.NewBatch(aggSchema, res))
	if err != nil {
		return err
	}

	return a.Next.Finish(ctx)
}

func (a *HashAggregate) encodeCols(cols []any) string {
	res := ""
	for _, col := range cols {
		res += fmt.Sprint(col)
	}
	return res
}

// ----------- OrderedAggregate -------------

type OrderedAggregate struct {
	Next PhysicalPlan

	GroupByList []eval_expr.Expr
	AggExprList []eval_expr.AggregateExpr

	// groupedData tree[string]hashTuple
	// This groupedData could be stored in an ordered tree. Need to learn more about it.
}

func NewOrderedAggregate(groupBy []eval_expr.Expr, aggExpr []eval_expr.AggregateExpr) PhysicalPlan {
	agg := OrderedAggregate{
		GroupByList: groupBy,
		AggExprList: aggExpr,
	}
	return &agg
}

func (o *OrderedAggregate) Schema() containers.ISchema {
	//TODO implement me
	panic("implement me")
}

func (o *OrderedAggregate) Children() []PhysicalPlan {
	//TODO implement me
	panic("implement me")
}

func (o *OrderedAggregate) Callback(ctx context.Context, batch containers.IBatch) error {
	//TODO implement me
	panic("implement me")
}

func (o *OrderedAggregate) SetNext(next PhysicalPlan) {
	//TODO implement me
	panic("implement me")
}

func (o *OrderedAggregate) Finish(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (o *OrderedAggregate) Execute(ctx *execution.TaskContext, callback tableprovider.Callback) error {
	//TODO implement me
	panic("implement me")
}
