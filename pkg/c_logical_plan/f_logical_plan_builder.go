package logicalplan

import (
	"tiny_dataframe/pkg/d_physicalplan/c_table_provider"
)

type Builder struct {
	plan LogicalPlan
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) Input(path string, source tableprovider.TableReader, proj []string) *Builder {
	b.plan = Input{Path: path, Source: source, Projection: proj}
	return b
}

func (b *Builder) Project(expr ...Expr) *Builder {
	b.plan = Projection{b.plan, expr}
	return b
}

func (b *Builder) Filter(pred Expr) *Builder {
	b.plan = Selection{b.plan, pred}
	return b
}

func (b *Builder) Aggregate(groupBy []Expr, aggExpr []AggregateExpr) *Builder {
	b.plan = Aggregate{b.plan, groupBy, aggExpr}
	return b
}

func (b *Builder) Output(callback tableprovider.Callback) *Builder {
	b.plan = Output{Next: b.plan, Callback: callback}
	return b
}

func (b *Builder) Build() (LogicalPlan, error) {
	if err := Validate(b.plan); err != nil {
		return nil, err
	}
	return b.plan, nil
}

func (b *Builder) Clone() *Builder {
	var copiedPlan LogicalPlan
	if b.plan != nil {
		copiedPlan = b.plan
	}
	return &Builder{plan: copiedPlan}
}

func Validate(plan LogicalPlan) error {
	return nil
}
