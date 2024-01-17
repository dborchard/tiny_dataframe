package operators

import (
	"context"
	"golang.org/x/sync/errgroup"
	"strings"
	execution "tiny_dataframe/pkg/e_exec_runtime"
	datasource "tiny_dataframe/pkg/f_data_source"
	containers "tiny_dataframe/pkg/g_containers"
)

//----------------- Input -----------------

type Input struct {
	Source datasource.TableReader

	//TODO: make this Expr instead of string
	// Add more things like Distinct or Filter etc.
	Projection []string
	next       PhysicalPlan
}

func (s *Input) SetNext(next PhysicalPlan) {
	s.next = next
}

func (s *Input) Callback(ctx context.Context, r containers.IBatch) error {
	panic("bug")
}

func (s *Input) Schema() containers.ISchema {
	if len(s.Projection) == 0 {
		return s.Source.Schema()
	}
	schema := s.Source.Schema()
	return schema.Select(s.Projection)
}

func (s *Input) Execute(ctx *execution.TaskContext, _ datasource.Callback) error {

	childrenCallbacks := make([]datasource.Callback, 0, len(s.Children()))
	for _, plan := range s.Children() {
		childrenCallbacks = append(childrenCallbacks, plan.Callback)
	}

	options := []datasource.Option{
		datasource.WithProjection(s.Projection...),
	}

	// For Push based functions.
	errGroup, _ := errgroup.WithContext(ctx.Ctx)
	errGroup.Go(func() error {
		return s.Source.View(ctx, func(ctx *execution.TaskContext, snapshotTs uint64) error {
			return s.Source.Push(ctx, snapshotTs, childrenCallbacks, options...)
		})
	})
	if err := errGroup.Wait(); err != nil {
		return err
	}

	// For AggFunc
	errGroup, _ = errgroup.WithContext(ctx.Ctx)
	for _, plan := range s.Children() {
		plan := plan
		errGroup.Go(func() (err error) {
			return plan.Finish(ctx.Ctx)
		})
	}

	return errGroup.Wait()
}

func (s *Input) Children() []PhysicalPlan {
	return []PhysicalPlan{s.next}
}

func (s *Input) String() string {
	schema := s.Schema()
	return "Input: schema=" + schema.String() + ", projection=" + strings.Join(s.Projection, ",")
}

func (s *Input) Finish(ctx context.Context) error {
	panic("bug")
}

// --------Output---------

type Output struct {
	OutputCallback datasource.Callback
}

func (e *Output) Schema() containers.ISchema {
	panic("bug")
}

func (e *Output) Children() []PhysicalPlan {
	panic("bug")
}

func (e *Output) Callback(ctx context.Context, r containers.IBatch) error {
	return e.OutputCallback(ctx, r)
}

func (e *Output) Execute(ctx *execution.TaskContext, callback datasource.Callback) error {
	panic("bug")
}

func (e *Output) SetNext(next PhysicalPlan) {
	panic("bug")
}

func (e *Output) Finish(ctx context.Context) error {
	return nil
}
