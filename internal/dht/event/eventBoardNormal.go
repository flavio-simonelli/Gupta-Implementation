package event

import (
	"GuptaDHT/internal/dht/routingtable"
	"GuptaDHT/internal/logger"
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// NormalBoard is a struct that incapsule the event board for normal node (unit leader or normal node).
type NormalBoard struct {
	board         *Board             // buffer FIFO
	retryInterval time.Duration      // interval for retrying to send events
	retryRunning  atomic.Bool        // indicates if the retry loop is running
	retryCancel   context.CancelFunc // context cancellation function to stop the retry loop (for shutdown node)
}

// newNormalBoard creates a new NormalBoard instance.
func newNormalBoard(retryInterval time.Duration) *NormalBoard {
	return &NormalBoard{
		board:         NewBoard(),
		retryInterval: retryInterval,
	}
}

func (nb *NormalBoard) resolveSliceLeaderAddress(table *routingtable.Table) (string, error) {
	sliceLeader, err := table.GetMySliceLeader()
	if err != nil {
		switch {
		case errors.Is(err, routingtable.ErrSliceLeaderNotFound):
			logger.Log.Errorf("Slice leader not found: %v", err)
			return "", err
		case errors.Is(err, routingtable.SliceLeaderItself):
			selfAddr := table.GetSelf().Address
			logger.Log.Warnf("Node is slice leader itself. Using self address: %s", selfAddr)
			return selfAddr, nil
		default:
			logger.Log.Errorf("Unexpected error determining slice leader: %v", err)
			return "", err
		}
	}
	logger.Log.Infof("Slice leader resolved: %s", sliceLeader.Address)
	return sliceLeader.Address, nil
}

func (nb *NormalBoard) enqueueWithRetry(event *Event, client EventSender, table *routingtable.Table) {
	nb.board.add(event)
	nb.startRetryLoop(client, table)
}

// SendEvent send an event to the slice leader or enqueues it if the send fails.
func (nb *NormalBoard) sendEvent(event *Event, client EventSender, table *routingtable.Table) {
	if !nb.board.isEmpty() {
		nb.enqueueWithRetry(event, client, table)
		return
	}
	logger.Log.Infof("Event board is empty. Attempting to send event.")
	events := []*Event{event}
	slAddress, err := nb.resolveSliceLeaderAddress(table)
	if err != nil {
		nb.enqueueWithRetry(event, client, table)
		return
	}
	if err := client.SendToSliceLeader(events, table.GetSelf().ID, slAddress); err == nil {
		return
	}
	logger.Log.Warnf("Send to slice leader failed: %v. Enqueuing event.", err)
	nb.enqueueWithRetry(event, client, table)
}

// startRetryLoop starts the retry loop for sending events.
func (nb *NormalBoard) startRetryLoop(client EventSender, table *routingtable.Table) {
	if !nb.retryRunning.CompareAndSwap(false, true) {
		return // già in esecuzione
	}
	var ctx context.Context
	ctx, nb.retryCancel = context.WithCancel(context.Background())
	go nb.retryLoop(ctx, client, table)
}

func (nb *NormalBoard) retryLoop(ctx context.Context, client EventSender, table *routingtable.Table) {
	ticker := time.NewTicker(nb.retryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Log.Infof("Retry loop stopped by context cancellation")
			return
		case <-ticker.C:
			if nb.board.isEmpty() {
				nb.stopRetryLoop()
				return
			}
			events := nb.board.getAll()
			address, err := nb.resolveSliceLeaderAddress(table)
			if err != nil {
				logger.Log.Warnf("Retry: impossibile risolvere slice leader: %v", err)
				continue
			}
			if err := client.SendToNormalNode(events, table.GetSelf().ID, address); err != nil {
				logger.Log.Warnf("Retry: invio fallito a %s: %v", address, err)
			} else {
				logger.Log.Infof("Retry: invio riuscito a %s", address)
				nb.board.clear()
			}
		}
	}
}

func (nb *NormalBoard) stopRetryLoop() {
	if nb.retryRunning.Load() {
		nb.retryCancel() // segnala alla goroutine di fermarsi
		nb.retryRunning.Store(false)
	}
}
