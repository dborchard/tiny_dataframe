package eval_expr

import (
	"fmt"
	containers "tiny_dataframe/pkg/g_containers"
)

type AggregateExpr struct {
	Name string
	Expr Expr
}

func (a AggregateExpr) Evaluate(input containers.IBatch) (containers.IVector, error) {
	panic("aggFunc Evaluate should not be called. It is handled by Pull based operation in physical plan")
}

func (a AggregateExpr) String() string {
	return a.Name + "(" + a.Expr.String() + ")"
}

func (a AggregateExpr) CreateAccumulator() Accumulator {
	switch a.Name {
	case "sum":
		return &SumAccumulator{}
	default:
		panic(fmt.Sprintf("Aggregate function %s is not implemented", a.Name))
	}
}

// ---------------------------------------------Accumulator Expressions---------------------------------------------

type Accumulator interface {
	Accumulate(val any)
	FinalValue() any
}

type SumAccumulator struct {
	val any
}

func (s *SumAccumulator) Accumulate(val any) {
	if val != nil {
		if s.val == nil {
			s.val = val
		} else {
			switch v := s.val.(type) {
			case int8:
				s.val = v + val.(int8)
			case int16:
				s.val = v + val.(int16)
			case int32:
				s.val = v + val.(int32)
			case int64:
				s.val = v + val.(int64)
			case uint8:
				s.val = v + val.(uint8)
			case uint16:
				s.val = v + val.(uint16)
			case uint32:
				s.val = v + val.(uint32)
			case uint64:
				s.val = v + val.(uint64)
			case float32:
				s.val = v + val.(float32)
			case float64:
				s.val = v + val.(float64)
			default:
				panic(fmt.Sprintf("Sum is not implemented for type: %T", v))
			}
		}
	}
}

func (s *SumAccumulator) FinalValue() any {
	return s.val
}
