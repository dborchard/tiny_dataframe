package engine

import (
	"time"
	dataframe "tiny_dataframe/pkg/b_dataframe"
	physicalplan "tiny_dataframe/pkg/d_physicalplan"
	"tiny_dataframe/pkg/d_physicalplan/c_table_provider"
	containers "tiny_dataframe/pkg/g_containers"
)

type ExecContext struct {
	SessionID string
	State     *physicalplan.ExecState
}

func NewContext() *ExecContext {
	sessionId := "session_" + time.Now().String()
	return &ExecContext{
		SessionID: sessionId,
		State:     physicalplan.NewExecState(sessionId),
	}
}

func (c *ExecContext) Parquet(path string, schema containers.ISchema) (dataframe.IDataFrame, error) {
	src, err := tableprovider.NewParquetTableReader(path, schema)
	if err != nil {
		return nil, err
	}

	return dataframe.NewDataFrame(c.State).Scan(path, src, nil), nil
}
