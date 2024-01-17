package containers

import (
	"errors"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type IVector interface {
	DataType() arrow.DataType
	GetValue(i int) any
	Len() int
	String() string
	GetArrowArray() arrow.Array
	Append(vec IVector) error
}

var _ IVector = &Vector{}

type Vector struct {
	src arrow.Array
}

func NewConstVector(arrowType arrow.DataType, size int, value any) IVector {
	col := make([]any, size)
	for i := 0; i < size; i++ {
		col[i] = value
	}
	return NewVector(arrowType, col)
}

func NewVector(arrowType arrow.DataType, data []any) IVector {
	allocator := memory.NewGoAllocator()
	builder := array.NewBuilder(allocator, arrowType)
	defer builder.Release()

	switch arrowType.(type) {
	case *arrow.Int64Type:
		intBuilder := builder.(*array.Int64Builder)
		for _, value := range data {
			v, ok := value.(int64)
			if !ok {
				panic("unsupported type")
			}
			intBuilder.Append(v)
		}
	case *arrow.Float64Type:
		floatBuilder := builder.(*array.Float64Builder)
		for _, value := range data {
			v, ok := value.(float64)
			if !ok {
				panic("unsupported type")
			}
			floatBuilder.Append(v)
		}
	case *arrow.BooleanType:
		boolBuilder := builder.(*array.BooleanBuilder)
		for _, value := range data {
			v, ok := value.(bool)
			if !ok {
				panic("unsupported type")
			}
			boolBuilder.Append(v)
		}
	case *arrow.StringType:
		stringBuilder := builder.(*array.StringBuilder)
		for _, value := range data {
			v, ok := value.(string)
			if !ok {
				panic("unsupported type")
			}
			stringBuilder.Append(v)
		}
	default:
		panic("unsupported type")
	}

	dataArr := builder.NewArray()
	return &Vector{src: dataArr}
}

func (v *Vector) DataType() arrow.DataType {
	return v.src.DataType()
}

func (v *Vector) GetValue(i int) any {
	return v.src.GetOneForMarshal(i)
}

func (v *Vector) Len() int {
	return v.src.Len()
}

func (v *Vector) String() string {
	return v.src.String()
}

func (v *Vector) GetArrowArray() arrow.Array {
	return v.src
}

func (v *Vector) Append(v2 IVector) error {
	pool := memory.NewGoAllocator()

	if v.src.DataType().ID() != v2.DataType().ID() {
		return errors.New("cannot append vectors of different data types")
	}

	var builder array.Builder
	switch v.src.DataType().ID() {
	case arrow.INT32:
		builder = array.NewInt32Builder(pool)
		arr1 := v.src.(*array.Int32)
		arr2 := v2.GetArrowArray().(*array.Int32)
		builder.(*array.Int32Builder).AppendValues(arr1.Int32Values(), nil)
		builder.(*array.Int32Builder).AppendValues(arr2.Int32Values(), nil)
	case arrow.INT64:
		builder = array.NewInt64Builder(pool)
		arr1 := v.src.(*array.Int64)
		arr2 := v2.GetArrowArray().(*array.Int64)
		builder.(*array.Int64Builder).AppendValues(arr1.Int64Values(), nil)
		builder.(*array.Int64Builder).AppendValues(arr2.Int64Values(), nil)
	default:
		return errors.New("unsupported data type")
	}

	combinedArray := builder.NewArray()
	builder.Release()

	v.src.Release()
	v.src = combinedArray

	return nil
}
