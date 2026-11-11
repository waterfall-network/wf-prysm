package blockchain

import (
	"context"

	"github.com/pkg/errors"
)

var (
	// errNilJustifiedInStore is returned when a nil justified checkpt is returned from store.
	errNilJustifiedInStore = errors.New("nil justified checkpoint returned from store")
	// errNilBestJustifiedInStore is returned when a nil justified checkpt is returned from store.
	errNilBestJustifiedInStore = errors.New("nil best justified checkpoint returned from store")
	// errNilFinalizedInStore is returned when a nil finalized checkpt is returned from store.
	errNilFinalizedInStore = errors.New("nil finalized checkpoint returned from store")
	// errInvalidNilSummary is returned when a nil summary is returned from the DB.
	errInvalidNilSummary = errors.New("nil summary returned from the DB")
	// errNilParentInDB is returned when a nil parent block is returned from the DB.
	errNilParentInDB = errors.New("nil parent block in DB")
	// errWrongBlockCount is returned when the wrong number of blocks or
	// block roots is used
	errWrongBlockCount = errors.New("wrong number of blocks or block roots")
	// errNoCoordState gwat coordinated state is not initialized or broken
	errNoCoordState = errors.New("no gwat coordinated state")
	// errAllSpinesLimitExceeded  is returned if length of all spines in spineData is exceeded.
	errAllSpinesLimitExceeded = errors.New("exceeded AllSpinesLimit")
	//errNoOptSpines
	errNoOptSpines = errors.New("no optimistic spines")
)

// invalidBlockError marks an error caused by the block content itself,
// as opposed to a node-side failure (timeout, missing state, db or gwat errors).
type invalidBlockError struct {
	error
}

func (e invalidBlockError) Unwrap() error {
	return e.error
}

// invalidBlock wraps err as a block validity failure, unless it is a context cancellation.
func invalidBlock(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return invalidBlockError{error: err}
}

// IsInvalidBlock returns true if the block was rejected because of its content.
// Only such blocks have to be marked as bad: a node-side failure must not
// cut off the block and all its descendants until the node restart.
func IsInvalidBlock(err error) bool {
	var e invalidBlockError
	return errors.As(err, &e)
}
