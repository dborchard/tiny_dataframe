package operators

import (
	"context"
	"fmt"
	"tiny_dataframe/pkg/d_physicalplan/eval_expr"
	execution "tiny_dataframe/pkg/e_exec_runtime"
	datasource "tiny_dataframe/pkg/f_data_source"
	containers "tiny_dataframe/pkg/g_containers"
)

/*
One reason to keep logical and physical plans separate is that sometimes there can be multiple ways
to execute a particular operation, meaning that there is a one-to-many relationship between logical
plans and physical plans.

For example, there could be separate physical plans for single-process versus distributed execution,
or CPU versus GPU execution.
Logical Plan describes what you want. Physical Plan describes how you want to do it.
In Physical Plan, you can have multiple ways to actually do it.
*/

type PhysicalPlan interface {
	Schema() containers.ISchema
	Children() []PhysicalPlan

	// Callback is used by the parent to send data to the child.
	// Used by Output, Projection, Selection
	Callback(ctx context.Context, batch containers.IBatch) error
	SetNext(next PhysicalPlan)

	// Finish is used by aggFn.
	Finish(ctx context.Context) error

	// Execute is only valid for DataSource, ie Input
	Execute(ctx *execution.TaskContext, callback datasource.Callback) error
}

var _ PhysicalPlan = &Input{}
var _ PhysicalPlan = &Output{}

var _ PhysicalPlan = &Projection{}
var _ PhysicalPlan = &Selection{}
var _ PhysicalPlan = &HashAggregate{}

//----------------- Projection -----------------

type Projection struct {
	Next PhysicalPlan
	Sch  containers.ISchema
	Proj []eval_expr.Expr
}

func (p *Projection) SetNext(next PhysicalPlan) {
	p.Next = next
}

func (p *Projection) Callback(ctx context.Context, batch containers.IBatch) error {
	vectors := make([]containers.IVector, len(p.Proj))
	var err error
	for colIdx, expr := range p.Proj {
		vectors[colIdx], err = expr.Evaluate(batch)
		if err != nil {
			return err
		}
	}
	return p.Next.Callback(ctx, containers.NewBatch(p.Sch, vectors))
}

func (p *Projection) String() string {
	return fmt.Sprintf("Projection: %s", p.Proj)
}

func (p *Projection) Schema() containers.ISchema {
	return p.Sch
}

func (p *Projection) Execute(_ *execution.TaskContext, _ datasource.Callback) error {
	panic("bug if you see this")
}

func (p *Projection) Children() []PhysicalPlan {
	return []PhysicalPlan{p.Next}
}

func (p *Projection) Finish(ctx context.Context) error {
	return p.Next.Finish(ctx)
}

//----------------- Selection -----------------

type Selection struct {
	Sch    containers.ISchema
	Next   PhysicalPlan
	Filter eval_expr.Expr
}

func (s *Selection) SetNext(next PhysicalPlan) {
	s.Next = next
}

func (s *Selection) Callback(ctx context.Context, batch containers.IBatch) error {
	sel, err := s.Filter.Evaluate(batch)
	if err != nil {
		return err
	}
	batch.Shrink(sel)
	return s.Next.Callback(ctx, batch)
}

func (s *Selection) Schema() containers.ISchema {
	return s.Sch
}

func (s *Selection) Children() []PhysicalPlan {
	return []PhysicalPlan{s.Next}
}

func (s *Selection) Execute(ctx *execution.TaskContext, callback datasource.Callback) error {
	panic("bug if you see this")
}

func (s *Selection) Finish(ctx context.Context) error {
	return s.Next.Finish(ctx)
}
