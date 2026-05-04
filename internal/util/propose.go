package util

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/hashicorp/raft"
)

var ErrPropose = errors.New("propose error")

// ProposeFunc egy callback, amivel az fsm-nek tudunk benyújtani új parancsot
// anélkük, hogy egy *raft.Raft példányt tároljunk minden egyes structunkban
type ProposeFunc func(ctx context.Context, cmd command.Command) (*command.Result, error)

const applyTimeout = 100 * time.Millisecond

func NewProposeFunc(r *raft.Raft) ProposeFunc {
	return func(ctx context.Context, cmd command.Command) (*command.Result, error) {
		bytes, err := command.Encode(cmd)
		if err != nil {
			return nil, fmt.Errorf("%w: failed: %w", ErrPropose, err)
		}
		applyResult, err := WaitApply(ctx, r.Apply(bytes, applyTimeout))
		if err != nil {
			return nil, fmt.Errorf("%w: waiting on ApplyFuture failed: %w", ErrPropose, err)
		}
		result, ok := applyResult.(command.Result)
		if !ok {
			return nil, fmt.Errorf("%w: unexpected result type", ErrPropose)

		}
		return &result, nil
	}
}
