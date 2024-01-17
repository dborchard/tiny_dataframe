package datasource

import (
	"context"
	"fmt"
	"testing"
	"time"
	execution "tiny_dataframe/pkg/e_exec_runtime"
	containers "tiny_dataframe/pkg/g_containers"
)

func TestParquetDataSource_Scan(t *testing.T) {
	ds, err := NewParquetDataSource("../../test/data/c1_c2_c3_int64.parquet", nil)
	if err != nil {
		t.Error(err)
	}

	err = ds.Push(&execution.TaskContext{Ctx: context.Background()},
		uint64(time.Now().UnixNano()),
		[]Callback{func(ctx context.Context, r containers.IBatch) error {
			fmt.Println(r)
			return nil
		}},
		WithProjection("c1", "c2", "c3"))

	if err != nil {
		t.Error(err)
	}
}
