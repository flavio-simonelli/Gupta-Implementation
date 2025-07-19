package event

import (
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/dht/routingtable"
	"errors"
	"time"
)

// ----- SliceLeader EventDispatcher -----

type SliceLeaderDispatcher struct {
	// Buffer per ogni slice, cioè per ogni unità (indice = sliceID)
	sliceBuffers []*EventBoard
	// Buffer per ogni unità (unit leader)
	unitBuffers []*EventBoard
	// routingTable per risolvere leader attuale di slice e unità
	table  *routingtable.Table
	client EventSender
	tbig   time.Duration
	twait  time.Duration
}

// NewSliceLeaderDispatcher creates a new SliceLeaderDispatcher instance.
func NewSliceLeaderDispatcher(client EventSender, table *routingtable.Table, tbig, twait time.Duration) *SliceLeaderDispatcher {
	sliceBuffers := make([]*EventBoard, id.GetK())
	for i := range sliceBuffers {
		sliceBuffers[i] = NewEventBoard()
	}

	unitBuffers := make([]*EventBoard, id.GetU())
	for i := range unitBuffers {
		unitBuffers[i] = NewEventBoard()
	}

	return &SliceLeaderDispatcher{
		sliceBuffers: sliceBuffers,
		unitBuffers:  unitBuffers,
		table:        table,
		client:       client,
		tbig:         tbig,
		twait:        twait,
	}
}

// AddEvent adds an event on the all buffer tranne il buffer della slice leader che lo ha inviato mettere sliceID < 0 se ad inviare un  evento non è uno slice leader
func (sld *SliceLeaderDispatcher) AddEvent(ev []*Event, sliceID int) error {
	// Validazione semplice
	if ev == nil || len(ev) == 0 {
		return errors.New("no events to add")
	}
	// Aggiungi eventi a tutti i unit leader buffer
	for _, board := range sld.unitBuffers {
		for _, e := range ev {
			board.AddEvent(e)
		}
	}
	// Aggiungi eventi a tutti gli altri slice leader buffer (tranne mittente se noto)
	for idx, board := range sld.sliceBuffers {
		if sliceID >= 0 && idx == sliceID {
			continue // skip sender
		}
		for _, e := range ev {
			board.AddEvent(e)
		}
	}
	return nil
}

/*
func (sld *SliceLeaderDispatcher) StartTbigLoop(ctx context.Context, batchSize int) {
	ticker := time.NewTicker(sld.tbig)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				for sliceID, buffer := range sld.sliceBuffers {
					addr := sld.table.(sliceID)
					events := buffer.PeekN(batchSize)

					if len(events) == 0 {
						continue
					}

					err := sld.client.SendToSliceLeader(events, addr)
					if err == nil {
						buffer.Commit(len(events))
					} else {
						// log.Printf("Failed to send to slice %d: %v", sliceID, err)
					}
				}
			}
		}
	}()
}

func (sld *SliceLeaderDispatcher) StartTwaitLoop(ctx context.Context, batchSize int) {
	ticker := time.NewTicker(sld.twait)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				for unitID, buffer := range sld.unitBuffers {
					addr := sld.table.GetUnitLeaderAddress(unitID)
					events := buffer.PeekN(batchSize)

					if len(events) == 0 {
						continue
					}

					err := sld.client.SendToUnitLeader(events, addr)
					if err == nil {
						buffer.Commit(len(events))
					} else {
						// log.Printf("Failed to send to unit %d: %v", unitID, err)
					}
				}
			}
		}
	}()
}
*/
