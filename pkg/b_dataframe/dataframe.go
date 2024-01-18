package dataframe

import (
	"context"
	"github.com/olekukonko/tablewriter"
	"os"
	rbo "tiny_dataframe/pkg/c_logical_plan/optimizer"
	operators "tiny_dataframe/pkg/d_physicalplan/b_operators"
	"tiny_dataframe/pkg/d_physicalplan/c_table_provider"
	execution "tiny_dataframe/pkg/e_exec_runtime"

	logicalplan "tiny_dataframe/pkg/c_logical_plan"
	phyiscalplan "tiny_dataframe/pkg/d_physicalplan"
	containers "tiny_dataframe/pkg/g_containers"
)

type IDataFrame interface {
	Scan(path string, source tableprovider.TableReader, proj []string) IDataFrame
	Project(expr ...logicalplan.Expr) IDataFrame
	Filter(expr logicalplan.Expr) IDataFrame
	Aggregate(groupBy []logicalplan.Expr, aggregateExpr []logicalplan.AggregateExpr) IDataFrame

	TaskContext() *execution.TaskContext
	Show() error

	LogicalPlan() (logicalplan.LogicalPlan, error)
	OptimizedLogicalPlan() (logicalplan.LogicalPlan, error)
	PhysicalPlan() (operators.PhysicalPlan, error)
}

type DataFrame struct {
	sessionState       *phyiscalplan.ExecState
	planBuilder        *logicalplan.Builder
	ruleBasedOptimizer *rbo.Optimizer
}

func NewDataFrame(sessionState *phyiscalplan.ExecState) IDataFrame {
	return &DataFrame{
		sessionState:       sessionState,
		planBuilder:        logicalplan.NewBuilder(),
		ruleBasedOptimizer: rbo.NewOptimizer(),
	}
}

func (df *DataFrame) Scan(path string, source tableprovider.TableReader, proj []string) IDataFrame {
	df.planBuilder.Input(path, source, proj)
	return df
}

func (df *DataFrame) Project(proj ...logicalplan.Expr) IDataFrame {
	df.planBuilder.Project(proj...)
	return df
}

func (df *DataFrame) Filter(predicate logicalplan.Expr) IDataFrame {
	df.planBuilder.Filter(predicate)
	return df
}

func (df *DataFrame) Aggregate(groupBy []logicalplan.Expr, aggExpr []logicalplan.AggregateExpr) IDataFrame {
	df.planBuilder.Aggregate(groupBy, aggExpr)
	return df
}

func (df *DataFrame) collect(ctx context.Context, callback tableprovider.Callback) error {
	// create a copy of the plan builder and add the output operator
	// NOTE: This is a hack to add Output operator to the PhysicalPlan.
	builder := df.planBuilder.Clone().Output(callback)

	// build the logical plan
	plan, err := builder.Build()
	if err != nil {
		return err
	}

	// optimize the logical plan
	plan = df.ruleBasedOptimizer.Optimize(plan)

	// create the physical plan
	physicalPlan, err := df.sessionState.CreatePhysicalPlan(plan)
	if err != nil {
		return err
	}

	// execute the physical plan
	return physicalPlan.Execute(df.TaskContext(), callback)
}

func (df *DataFrame) TaskContext() *execution.TaskContext {
	return df.sessionState.TaskContext()
}

func (df *DataFrame) LogicalPlan() (logicalplan.LogicalPlan, error) {
	return df.planBuilder.Build()
}

// OptimizedLogicalPlan returns the optimized logical plan. This is mainly for testing only.
func (df *DataFrame) OptimizedLogicalPlan() (logicalplan.LogicalPlan, error) {
	plan, err := df.LogicalPlan()
	if err != nil {
		return nil, err
	}
	return df.ruleBasedOptimizer.Optimize(plan), nil
}

func (df *DataFrame) Show() error {

	batches := make([]containers.IBatch, 0)
	var schema containers.ISchema
	err := df.collect(context.TODO(), func(ctx context.Context, batch containers.IBatch) error {
		if schema == nil {
			schema = batch.Schema()
		}
		batches = append(batches, batch)
		return nil
	})

	if err != nil {
		return err
	}
	if len(batches) == 0 {
		return nil
	}

	table := tablewriter.NewWriter(os.Stdout)

	// 1. add headers
	headers := make([]string, 0)
	for _, field := range schema.Fields() {
		headers = append(headers, field.Name)
	}
	table.SetHeader(headers)

	// 2. add data
	for _, batch := range batches {
		table.AppendBulk(batch.StringTable())
	}

	// 3. render
	table.Render()
	return nil
}

func (df *DataFrame) PhysicalPlan() (operators.PhysicalPlan, error) {
	plan, err := df.OptimizedLogicalPlan()
	if err != nil {
		return nil, err
	}

	return df.sessionState.CreatePhysicalPlan(plan)
}
