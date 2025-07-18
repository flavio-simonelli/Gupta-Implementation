package routingtable

import "GuptaDHT/internal/dht/id"

// questa è l'interfaccia delle entry che viene utilizzata dall'esterno del package per interagire con le entry della routing table

type Entry struct {
	ID      id.ID
	Address string
	IsSN    bool // is supernode
	IsUL    bool // is unit leader
	IsSL    bool // is slice leader
}

// toPublicEntry converts a routingEntry to a public Entry.
func toPublicEntry(re *routingEntry, sn, ul, sl bool) Entry {
	return Entry{
		ID:      re.GetID(),
		Address: re.GetAddress(),
		IsSN:    sn,
		IsUL:    ul,
		IsSL:    sl,
	}
}
