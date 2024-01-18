package simple

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"tiny_dataframe/pkg/a_engine"
	logicalplan "tiny_dataframe/pkg/c_logical_plan"
)

func TestParquetFile(t *testing.T) {
	ctx := engine.NewContext()
	df, err := ctx.Parquet("../../test/data/c1_c2_c3_int64.parquet", nil)
	if err != nil {
		t.Error(err)
	}

	df = df.
		Filter(logicalplan.Eq(
			logicalplan.ColumnExpr{Name: "c1"},
			logicalplan.LiteralInt64Expr{Val: 200},
		)).
		Project(
			logicalplan.ColumnExpr{Name: "c1"},
			logicalplan.ColumnExpr{Name: "c2"},
		).Aggregate(
		[]logicalplan.Expr{
			logicalplan.ColumnExpr{Name: "c1"},
		},
		[]logicalplan.AggregateExpr{
			{
				Name: "sum",
				Expr: logicalplan.ColumnExpr{Name: "c2"},
			},
		})

	logicalPlan, _ := df.LogicalPlan()
	fmt.Println(logicalplan.PrettyPrint(logicalPlan, 0))
	assert.Equal(t, "Aggregate: groupExpr=[#c1], aggregateExpr=[sum(#c2)]\n\tProjection: #c1, #c2\n\t\tFilter: #c1 = 200\n\t\t\tInput: ../../test/data/c1_c2_c3_int64.parquet; projExpr=None\n", logicalplan.PrettyPrint(logicalPlan, 0))

	logicalPlan, _ = df.OptimizedLogicalPlan()
	fmt.Println(logicalplan.PrettyPrint(logicalPlan, 0))
	assert.Equal(t, "Aggregate: groupExpr=[#c1], aggregateExpr=[sum(#c2)]\n\tProjection: #c1, #c2\n\t\tFilter: #c1 = 200\n\t\t\tInput: ../../test/data/c1_c2_c3_int64.parquet; projExpr=[c1 c2]\n", logicalplan.PrettyPrint(logicalPlan, 0))

	err = df.Show()
	if err != nil {
		t.Error(err)
	}
}
