package event

import (
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/dht/routingtable"
	"GuptaDHT/internal/logger"
	"context"
	"fmt"
	"time"
)

type SliceLeaderBoard struct {
	// buffers
	sliceBoards              map[int]*Board // key: slice index
	unitBoards               map[int]*Board // key: unit index
	tBig                     time.Duration
	tWait                    time.Duration
	routineSliceBoardsCancel context.CancelFunc // context cancellation function to stop the routine for sliceBoards
	routineUnitBoardsCancel  context.CancelFunc // context cancellation function to stop the routine for unitBoards
}

// newSliceLeaderBoard creates a new SliceLeaderBoard instance.
func newSliceLeaderBoard(tBig, tWait time.Duration) *SliceLeaderBoard {
	board := &SliceLeaderBoard{
		tBig:  tBig,
		tWait: tWait,
	}
	return board
}

// initializeSliceLeaderBoard initializes the SliceLeaderBoard with the given parameters.
func (slb *SliceLeaderBoard) initializeSliceLeaderBoard(client EventSender, table *routingtable.Table) {
	// create the map for slice boards and unit boards
	slb.sliceBoards = make(map[int]*Board)
	slb.unitBoards = make(map[int]*Board)
	// get number of slices and units
	numSlices := id.GetK()
	numUnits := id.GetU()
	// get the slice of the node
	slice, _ := table.GetSelf().ID.SliceAndUnit()
	// initialize slice boards
	for i := 0; i < numSlices; i++ {
		if i == slice {
			// skip the slice of the node itself
			continue
		}
		slb.sliceBoards[i] = NewBoard()
	}
	// initialize unit boards
	for i := 0; i < numUnits; i++ {
		slb.unitBoards[i] = NewBoard()
	}
	// Start goroutines
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	slb.routineSliceBoardsCancel = cancel1
	slb.routineUnitBoardsCancel = cancel2

	go slb.startSliceBoardRoutine(ctx1, client, table)
	go slb.startUnitBoardRoutine(ctx2, client, table)
}

// addExternalEvents adds an event to the slice leader board for external events (e.g., from other slices).
func (slb *SliceLeaderBoard) addExternalEvents(ev []*Event, sliceID int) error {
	// validation of the event
	if ev == nil || len(ev) == 0 {
		return fmt.Errorf("event is empty")
	}
	// validation of the sliceID
	if sliceID < 0 || sliceID >= id.GetK() {
		return fmt.Errorf("invalid sliceID: %d", sliceID)
	}
	// add event to the slice board
	for idx, board := range slb.sliceBoards {
		if idx == sliceID {
			continue // skip the sender's slice board
		}
		board.addBatch(ev)
	}
	// add event to the unit boards
	for _, board := range slb.unitBoards {
		board.addBatch(ev)
	}
	return nil
}

// addEvents adds an event to the slice leader board for internal events (e.g., from the same slice).
func (slb *SliceLeaderBoard) addEvents(ev []*Event) error {
	// validation of the event
	if ev == nil || len(ev) == 0 {
		return fmt.Errorf("event is empty")
	}
	// add event to the slice board
	for _, board := range slb.sliceBoards {
		board.addBatch(ev)
	}
	// add event to the unit boards
	for _, board := range slb.unitBoards {
		board.addBatch(ev)
	}
	return nil
}

func (slb *SliceLeaderBoard) startSliceBoardRoutine(ctx context.Context, client EventSender, table *routingtable.Table) {
	ticker := time.NewTicker(slb.tBig)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for sliceIndex, board := range slb.sliceBoards {
				if board.isEmpty() {
					continue
				}
				sliceLeader, err := table.GetSliceLeader(sliceIndex)
				if err != nil {
					logger.Log.Warnf("Cannot resolve slice leader for slice %d: %v", sliceIndex, err)
					continue
				}
				events := board.getAll()
				// send events to the slice leader
				if err := client.SendToSliceLeader(events, table.GetSelf().ID, sliceLeader.Address); err != nil {
					logger.Log.Warnf("Failed to send to slice leader %d: %v", sliceIndex, err)
				} else {
					logger.Log.Infof("Sent %d events to slice leader %d", len(events), sliceIndex)
					board.clear() // clear the board after successful send
				}
			}
		}
	}
}

func (slb *SliceLeaderBoard) startUnitBoardRoutine(ctx context.Context, client EventSender, table *routingtable.Table) {
	ticker := time.NewTicker(slb.tWait)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sliceID, _ := table.GetSelf().ID.SliceAndUnit()
			for unitIndex, board := range slb.unitBoards {
				if board.isEmpty() {
					continue
				}
				unitLeader, err := table.GetUnitLeader(sliceID, unitIndex)
				if err != nil {
					logger.Log.Warnf("Cannot resolve unit leader for unit %d: %v", unitIndex, err)
					continue
				}
				events := board.getAll()
				// send events to the unit leader
				if err := client.SendToUnitLeader(events, table.GetSelf().ID, unitLeader.Address); err != nil {
					logger.Log.Warnf("Failed to send to unit leader %d: %v", unitIndex, err)
				} else {
					logger.Log.Infof("Sent %d events to unit leader %d", len(events), unitIndex)
					board.clear() // clear the board after successful send
				}
			}
		}
	}
}

func (slb *SliceLeaderBoard) stop() {
	if slb.routineSliceBoardsCancel != nil {
		slb.routineSliceBoardsCancel()
	}
	if slb.routineUnitBoardsCancel != nil {
		slb.routineUnitBoardsCancel()
	}
	slb.sliceBoards = nil
	slb.unitBoards = nil
}
