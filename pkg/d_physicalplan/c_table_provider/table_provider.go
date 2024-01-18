package tableprovider

import (
	"context"
	eval_expr "tiny_dataframe/pkg/d_physicalplan/a_eval_expr"
	execution "tiny_dataframe/pkg/e_exec_runtime"
	containers "tiny_dataframe/pkg/g_containers"
)

type TableReader interface {
	Schema() containers.ISchema
	View(ctx *execution.TaskContext, fn func(ctx *execution.TaskContext, snapshotTs uint64) error) error
	Push(ctx *execution.TaskContext, snapshotTs uint64, callbacks []Callback, options ...Option) error
}

var _ TableReader = &ParquetTableReader{}

type Callback func(ctx context.Context, batch containers.IBatch) error

type Option func(opts *IterOptions)

func WithProjection(e ...string) Option {
	return func(opts *IterOptions) {
		opts.Projection = append(opts.Projection, e...)
	}
}

// IterOptions are a set of options for the TableReader Iterators.
type IterOptions struct {
	// TODO: replace Projection with Exprs
	// Introduce Filter in Datasource level

	Projection []string
	Filter     []eval_expr.Expr
}
