package peer

import "crypto/md5"

/**
 * Package che definisce i metodi per la gestione degli ID nella DHT
 */

// ID rappresenta l'identificatore univoco di un Peer/risorsa nella rete DHT.
type ID [16]byte

// GetID calcola l'ID a partire da una stringa di contenuto tramite la funzione di crittografia MD5
func GetID(content string) ID {
	return ID(md5.Sum([]byte(content)))
}

// inInterval verifica se un ID è compreso in un intervallo definito come (start, end]
func (id ID) inInterval(start, end ID) bool {
	// se start < end: caso normale in cui l'intervallo non attraversa il punto di wrap-around
	if start.lessThan(end) {
		// verifichiamo che id>start e id<=end
		return start.lessThan(id) && (id.lessThan(end) || id.equals(end))
	}
	// se start >= end: caso in cui l'intervallo attraversa il wrap-around
	// verifichiamo che id>start o id<=end
	return start.lessThan(id) || (id.lessThan(end) || id.equals(end))
}

// Confronto < fra due ID
func (id ID) lessThan(b ID) bool {
	for i := 0; i < len(id); i++ {
		if id[i] < b[i] {
			return true
		} else if id[i] > b[i] {
			return false
		}
	}
	return false
}

// Confronto == fra due ID
func (id ID) equals(b ID) bool {
	for i := 0; i < len(id); i++ {
		if id[i] != b[i] {
			return false
		}
	}
	return true
}
