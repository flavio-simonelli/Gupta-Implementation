package peer

import "errors"

/**
 * File che definisce la struttura e le funzionalità della Finger Table di un Peer.
* La finger Table per questione di efficienza nella ricerca è implementata secondo un ordinamento crecente globale rispetto agli ID (non in base alla posizione nell'anello).
*/

// definizione degli errori
var ErrFingerTableEmpty = errors.New("FingerTable is empty")
var ErrEntryNotFound = errors.New("FingerEntry not found in FingerTable")

// FingerEntry per la tabella di routing.
type FingerEntry struct {
	ID       ID
	endPoint string
}

// FingerTable definisce una tabella di routing per un Peer nella rete DHT.
type FingerTable []FingerEntry

// FingerTableOperations Interfaccia per le operazioni sulla Finger Table
type FingerTableOperations interface {
	// AddEntry aggiunge un nuovo FingerEntry alla FingerTable.
	AddEntry(entry FingerEntry) error
	// RemoveEntry rimuove un FingerEntry dalla FingerTable.
	RemoveEntry(id ID) error
	// FindSuccessor trova il successore di un ID specificato nella FingerTable.
	FindSuccessor(id ID) (string, error)
}

// FindSuccessor trova il successore di un ID specificato nella FingerTable.
func (ft *FingerTable) FindSuccessor(id ID) (string, error) {
	// effettua una ricerca binaria all'interno della finger table (ordinata per ID) trovando il Successore, cioè il Peer con min{idPeer-id}>=0
	// controlla se la finger table è vuota
	if len(*ft) == 0 {
		return "", ErrFingerTableEmpty // ritorna un errore o un valore di default se la finger table è vuota
	}
	// Se l'id è maggiore dell'ultimo Id nella tabella, allora il successore è la prima entry (caso di wrap-around)
	if (*ft)[len(*ft)-1].ID.lessThan(id) {
		return (*ft)[0].endPoint, nil // ritorniamo il primo elemento della finger table
	}
	// altrimenti facciamo una ricerca binaria per trovare il successore
	idx, err := BinarysearchSuccessor(id, *ft)
	if err != nil {
		return "", err // ritorniamo l'errore se non troviamo il successore
	}
	// ritorniamo l'endpoint associato alla entry trovata
	return (*ft)[idx].endPoint, nil
}

// BinarysearchSuccessor effettua una ricerca binaria per trovare il successore di un ID specificato nella FingerTable.
func BinarysearchSuccessor(id ID, ft FingerTable) (int, error) {
	low, high := 0, len(ft)-1
	for low <= high {
		// prendiamo la entry centrale
		mid := low + (high-low)/2
		midId := ft[mid].ID
		// se midId < id iteriamo la procedura con la metà destra
		if midId.lessThan(id) {
			return BinarysearchSuccessor(id, ft[mid+1:])
		} else if midId.equals(id) {
			// se midId == id ritorniamo l'endpoint associato poichè siamo sicuri che sia il successore
			return mid, nil
		} else {
			// se midId > id il punto trovato è un possibile candidato ad essere il successore
			// verifichiamo che sia il più vicino possibile, cioè la entryID precedente deve essere minore di id
			if mid == 0 || ft[mid-1].ID.lessThan(id) {
				return mid, nil // ritorniamo l'endpoint associato alla entry corrente
			} else if ft[mid-1].ID.equals(id) {
				// la entry precedente ha lo stesso ID, quindi ritorniamo il suo endpoint
				return mid - 1, nil
			}
			// altrimenti iteriamo la procedura con la metà sinistra
			return BinarysearchSuccessor(id, ft[:mid])
		}
	}
	return -1, ErrEntryNotFound // errore nella ricerca, non è stato trovato un successore
}

/**
AddEntry aggiunge un nuovo FingerEntry alla FingerTable mantenendo la tabella ordinata in ordine crescente per ID.
Se la tabella è vuota, inerisce direttmente l'entry.
Se la tabella non è vuota effettua una ricerca binaria per trovare il punto di inserimento corretto.
*/

func (ft *FingerTable) AddEntry(entry FingerEntry) error {
	if len(*ft) == 0 {
		*ft = append(*ft, entry) // inserisce l'entry direttamente se la finger table è vuota
		return nil
	}
	// Effettua una ricerca binaria per trovare il punto di inserimento
	idxSucc, err := BinarysearchSuccessor(entry.ID, *ft)
	if err != nil {
		// Se non trovi un successore (cioè id maggiore di tutti), inserisci alla fine
		if errors.Is(err, ErrEntryNotFound) {
			*ft = append(*ft, entry)
			return nil
		}
		return err // ritorna l'errore se diverso
	}
	// Inserisce l'entry nella posizione corretta
	*ft = append(*ft, FingerEntry{})         // aumenta la lunghezza della slice
	copy((*ft)[idxSucc+1:], (*ft)[idxSucc:]) // sposta gli elementi a destra da idxSucc in poi
	(*ft)[idxSucc] = entry                   // inserisce l'entry nella posizione corretta
	return nil
}

// RemoveEntry
/*
RemoveEntry rimuove un FingerEntry dalla FingerTable.
Se la tabella è vuota, ritorna un errore.
Se la tabella non è vuota, effettua una ricerca binaria per trovare l'entry da rimuovere.
*/
func (ft *FingerTable) RemoveEntry(id ID) error {
	if len(*ft) == 0 {
		return ErrFingerTableEmpty // ritorna un errore se la finger table è vuota
	}
	// Effettua una ricerca binaria per trovare l'entry da rimuovere
	idx, err := BinarysearchSuccessor(id, *ft)
	if err != nil {
		return err // ritorna l'errore se non troviamo l'entry
	}
	// Rimuove l'entry trovata
	copy((*ft)[idx:], (*ft)[idx+1:]) // sposta gli elementi a sinistra
	*ft = (*ft)[:len(*ft)-1]         // riduce la lunghezza della slice
	return nil
}
