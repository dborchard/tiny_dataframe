package physicalplan

import (
	"context"
	"time"
	logicalplan "tiny_dataframe/pkg/c_logical_plan"
	"tiny_dataframe/pkg/d_physicalplan/operators"
	execution "tiny_dataframe/pkg/e_exec_runtime"
)

type ExecState struct {
	SessionID        string
	SessionStartTime time.Time
	QueryPlanner     QueryPlanner
	RuntimeEnv       *execution.RuntimeEnv
}

func NewExecState(sessionId string) *ExecState {
	return &ExecState{
		SessionID:        sessionId,
		SessionStartTime: time.Now(),
		QueryPlanner:     DefaultQueryPlanner{},
		RuntimeEnv:       execution.NewRuntimeEnv(),
	}
}

func (s ExecState) TaskContext() *execution.TaskContext {
	return &execution.TaskContext{
		SessionID: s.SessionID,
		TaskID:    time.Now().String(),
		Runtime:   s.RuntimeEnv,
		Ctx:       context.Background(),
	}
}

func (s ExecState) CreatePhysicalPlan(plan logicalplan.LogicalPlan) (operators.PhysicalPlan, error) {
	return s.QueryPlanner.CreatePhyPlan(plan, s)
}
