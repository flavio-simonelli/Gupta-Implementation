package routingtable

import "GuptaDHT/internal/dht/id"

// questa è l'interfaccia delle entry che viene utilizzata dall'esterno del package per interagire con le entry della routing table

type PublicEntry struct {
	ID      id.ID
	Address string
	IsSN    bool // is supernode
	IsUL    bool // is unit leader
	IsSL    bool // is slice leader
}

// toPublicEntry converts a routingEntry to a public PublicEntry.
func toPublicEntry(re *routingEntry, sn, ul, sl bool) PublicEntry {
	return PublicEntry{
		ID:      re.GetID(),
		Address: re.GetAddress(),
		IsSN:    sn,
		IsUL:    ul,
		IsSL:    sl,
	}
}

// NewPublicEntry creates a new PublicEntry from the given parameters.
func NewPublicEntry(id id.ID, address string, isSN, isUL, isSL bool) PublicEntry {
	return PublicEntry{
		ID:      id,
		Address: address,
		IsSN:    isSN,
		IsUL:    isUL,
		IsSL:    isSL,
	}
}
