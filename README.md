## Tiny Dataframe

### Why Dataframe

Dataframe removes the complexity of handling `SQL parsing`, `SQL rewriting`, `Binder/SQL Query Planner` etc. Once
the dataframe is mature, we can easily integrate it with an SQL engine.

### Features
- `Push based` query execution
- Abstraction over `arrow.Record`, `arrow.Array` and `arrow.Schema`
- Support `Parquet` reading with schema inference
- `Rule Based` Optimizer
- `AggFunc`: Sum
- `BooleanBinaryExpr`: Lt

### Example

```go
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

	_ = df.Show()
	/*
	   +-----+-----+-----+
	   | C1  | C2  | C3  |
	   +-----+-----+-----+
	   | 100 | 101 | 102 |
	   | 100 | 201 | 202 |
	   | 100 | 301 | 302 |
	   | 200 | 401 | 402 |
	   | 200 | 501 | 502 |
	   | 300 | 601 | 602 |
	   +-----+-----+-----+
	*/
	df = df.
		Filter(logicalplan.Lt(
			logicalplan.ColumnExpr{Name: "c1"},
			logicalplan.LiteralInt64Expr{Val: 300},
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
	assert.Equal(t, "Aggregate: groupExpr=[#c1], aggregateExpr=[sum(#c2)]\n\tProjection: #c1, #c2\n\t\tFilter: #c1 < 300\n\t\t\tInput: ../../test/data/c1_c2_c3_int64.parquet; projExpr=None\n", logicalplan.PrettyPrint(logicalPlan, 0))
	/*
		Aggregate: groupExpr=[#c1], aggregateExpr=[sum(#c2)]
			Projection: #c1, #c2
				Filter: #c1 < 300
					Input: ../../test/data/c1_c2_c3_int64.parquet; projExpr=None
	*/

	logicalPlan, _ = df.OptimizedLogicalPlan()
	fmt.Println(logicalplan.PrettyPrint(logicalPlan, 0))
	assert.Equal(t, "Aggregate: groupExpr=[#c1], aggregateExpr=[sum(#c2)]\n\tProjection: #c1, #c2\n\t\tFilter: #c1 < 300\n\t\t\tInput: ../../test/data/c1_c2_c3_int64.parquet; projExpr=[c1 c2]\n", logicalplan.PrettyPrint(logicalPlan, 0))
	/*
	   Aggregate: groupExpr=[#c1], aggregateExpr=[sum(#c2)]
	   	Projection: #c1, #c2
	   		Filter: #c1 < 300
	   			Input: ../../test/data/c1_c2_c3_int64.parquet; projExpr=[c1 c2]
	*/
	err = df.Show()
	if err != nil {
		t.Error(err)
	}
	/*
		+-----+---------+
		| #0  | SUM(#1) |
		+-----+---------+
		| 100 |     603 |
		| 200 |     902 |
		+-----+---------+
	*/
}
```

### Reference
- [FrostDB](https://github.com/polarsignals/frostdb) 
- [Arrow DataFusion](https://github.com/apache/arrow-datafusion) 
- [KQuery](https://github.com/dbminions/how-query-engine-work)
- [Drogo](https://github.com/dbminions/drogo)
