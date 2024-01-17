## Tiny Dataframe

Inspired by [FrostDB](https://github.com/polarsignals/frostdb)
and [Arrow DataFusion](https://github.com/apache/arrow-datafusion).

### Why Dataframe

Dataframe removes the complexity of handling `SQL parsing`, `SQL rewriting`, `Binding`, `SQL Query Planner` etc. Once
the dataframe is mature, we can easily integrate it with an SQL engine.


### Example

```go
package main

import (
	"fmt"
	"tiny_dataframe/pkg/a_engine"
	logicalplan "tiny_dataframe/pkg/c_logical_plan"
)

func main() {
	ctx := engine.NewContext()
	df, _ := ctx.Parquet("../../test/data/c1_c2_int64.parquet", nil)
	
	df = df.
		Project(
			logicalplan.Column{Name: "c1"},
			logicalplan.Column{Name: "c2"},
		).
		Filter(logicalplan.Eq(
			logicalplan.Column{Name: "c1"},
			logicalplan.LiteralInt64{Val: 100},
		))

	logicalPlan, _ := df.LogicalPlan()
	fmt.Println(logicalplan.PrettyPrint(logicalPlan, 0))
	/*
	Filter: #c1 = 100
		Projection: #c1, #c2
			Input: ../../test/data/c1_c2_int64.parquet; projExpr=None
	*/

	_ = df.Show()
	/*
	+-----+-----+
	| C1  | C2  |
	+-----+-----+
	| 100 | 101 |
	+-----+-----+
	*/
}

```