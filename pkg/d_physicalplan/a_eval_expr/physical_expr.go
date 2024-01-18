package eval_expr

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"strconv"
	containers "tiny_dataframe/pkg/g_containers"
)

type Expr interface {
	// TODO: replace Evaluate with 	`Accept(Visitor) bool`

	Evaluate(input containers.IBatch) (containers.IVector, error)
	String() string
}

var _ Expr = ColumnExpr{}

var _ Expr = BooleanBinaryExpr{}
var _ Expr = AggregateExpr{}

var _ Expr = LiteralStringExpr{}
var _ Expr = LiteralInt64Expr{}
var _ Expr = LiteralFloat64Expr{}

// var _ Expr = AliasExpr{}
// var _ Expr = MathExpr{}

// ----------- ColumnExpr -------------

type ColumnExpr struct {
	Index int
}

func (col ColumnExpr) Evaluate(input containers.IBatch) (containers.IVector, error) {
	return input.Column(col.Index), nil
}

func (col ColumnExpr) String() string {
	return "#" + strconv.Itoa(col.Index)
}

// ----------- LiteralInt64Expr -------------

type LiteralInt64Expr struct {
	Value int64
}

func (lit LiteralInt64Expr) String() string {
	return strconv.FormatInt(lit.Value, 10)
}

func (lit LiteralInt64Expr) Evaluate(input containers.IBatch) (containers.IVector, error) {
	return containers.NewConstVector(arrow.PrimitiveTypes.Int64, input.RowCount(), lit.Value), nil
}

// ----------- LiteralFloat64Expr -------------

type LiteralFloat64Expr struct {
	Value float64
}

func (lit LiteralFloat64Expr) String() string {
	return strconv.FormatFloat(lit.Value, 'f', -1, 64)
}

func (lit LiteralFloat64Expr) Evaluate(input containers.IBatch) (containers.IVector, error) {
	return containers.NewConstVector(arrow.PrimitiveTypes.Float64, input.RowCount(), lit.Value), nil
}

// ----------- LiteralStringExpr -------------

type LiteralStringExpr struct {
	Value string
}

func (lit LiteralStringExpr) Evaluate(input containers.IBatch) (containers.IVector, error) {
	return containers.NewConstVector(arrow.BinaryTypes.String, input.RowCount(), lit.Value), nil
}

func (lit LiteralStringExpr) String() string {
	return fmt.Sprintf("'%s'", lit.Value)
}

// ----------- BooleanBinaryExpr -------------

type BooleanBinaryExpr struct {
	L  Expr
	Op string
	R  Expr
}

func (e BooleanBinaryExpr) Evaluate(input containers.IBatch) (containers.IVector, error) {
	ll, err := e.L.Evaluate(input)
	if err != nil {
		return nil, err
	}
	rr, err := e.R.Evaluate(input)
	if err != nil {
		return nil, err
	}

	if ll.Len() != rr.Len() {
		return nil, fmt.Errorf("binary expression operands do not have the same length")
	}
	if ll.DataType() != rr.DataType() {
		return nil, fmt.Errorf("binary expression operands do not have the same type")
	}

	return e.evaluate(ll, rr)
}

func (e BooleanBinaryExpr) evaluate(l, r containers.IVector) (containers.IVector, error) {
	res := make([]any, 0)
	switch e.Op {
	case "<":
		switch l.DataType() {
		case arrow.PrimitiveTypes.Int64:
			for i := 0; i < l.Len(); i++ {
				if l.GetValue(i).(int64) < r.GetValue(i).(int64) {
					res = append(res, true)
				} else {
					res = append(res, false)
				}
			}
		}
		return containers.NewVector(arrow.FixedWidthTypes.Boolean, res), nil
	default:
		return nil, fmt.Errorf("unknown binary operator: %s", e.Op)
	}
}

func (e BooleanBinaryExpr) String() string {
	return e.L.String() + "+" + e.R.String()
}
