package keepalive

import (
	"GuptaDHT/internal/dht/routingtable"
	"GuptaDHT/internal/logger"
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// Package keepalive provides the functionality for detecting node failure
// in a distributed hash table (DHT) through periodic liveness checks.
//
// --- English ---
// The KeepAlive functionality enables nodes in the DHT to detect the failure of their neighboring nodes.
//
// According to the adopted protocol, each node is responsible for monitoring the liveness of its immediate successor.
// The `keepAliveInterval` defines the maximum time a node can remain inactive
// (i.e., without sending or receiving any messages from the successor) before being considered failed.
//
// Each node must send at least one message to its successor within this interval.
// When possible, the keep-alive message is piggybacked on other messages (e.g., dissemination messages)
// to minimize network traffic.
//
// The implementation runs as a goroutine started at node initialization.
// It wakes up periodically to check whether recent communication with the successor has occurred.
// If no message has been exchanged within `keepAliveInterval`, the goroutine actively sends a keep-alive message.
// If the message fails or no response is received, the successor is assumed to have failed,
// and the failure-handling procedure is triggered.
//
// --- Italiano ---
// La funzionalità di KeepAlive permette ai nodi della DHT di rilevare il fallimento dei nodi vicini.
//
// Secondo il protocollo adottato, ogni nodo mantiene il keep-alive del proprio successore diretto.
// Il parametro `keepAliveInterval` rappresenta l’intervallo massimo in cui un nodo può rimanere inattivo
// (cioè senza inviare o ricevere messaggi dal successore) prima di essere considerato fallito.
//
// Ogni nodo è responsabile di inviare almeno un messaggio al proprio successore entro questo intervallo.
// Quando possibile, il messaggio di keep-alive è "piggybacked" su altri messaggi (es. disseminazione),
// per ridurre il traffico nella rete.
//
// L’implementazione consiste in una goroutine che viene avviata all’accensione del nodo.
// Questa si risveglia periodicamente per verificare se il nodo ha comunicato recentemente con il successore.
// Se non sono stati scambiati messaggi entro `keepAliveInterval`, la goroutine invia attivamente
// un messaggio di keep-alive. In caso di errore o mancata risposta, il successore viene considerato fallito
// e si avvia la procedura di rimozione dal sistema.

// KeepAliveSender is the interface responsible for sending keep-alive messages to the successor.
type KeepAliveSender interface {
	// SendKeepAlive sends a keep-alive message to the specified receiver.
	SendKeepAlive(receiver string) error
}

// KeepAlive is the struct that implements the keep-alive functionality for DHT nodes.
type KeepAlive struct {
	net               KeepAliveSender     // Network interface to send keep-alive messages
	table             *routingtable.Table // Routing table to access the successor node
	keepAliveInterval time.Duration       // Interval at which keep-alive messages are sent
	lastSeen          atomic.Int64        // Timestamp of the last message received from the successor
	cancel            context.CancelFunc  // Context cancellation function to stop the keep-alive goroutine (for shutdown node)
}

// InitializeKeepAlive initializes the KeepAlive struct with the given parameters and start the goroutine.
func InitializeKeepAlive(net KeepAliveSender, table *routingtable.Table, keepAliveInterval time.Duration) *KeepAlive {
	ka := &KeepAlive{
		net:               net,
		table:             table,
		keepAliveInterval: keepAliveInterval,
	}
	// Start the keep-alive goroutine
	ctx, cancel := context.WithCancel(context.Background())
	go ka.start(ctx)
	ka.cancel = cancel

	return ka
}

// start is the goroutine that periodically checks for messages from the successor and sends keep-alive messages.
func (ka *KeepAlive) start(ctx context.Context) {
	for {
		var waitDuration time.Duration
		// Get the successor information
		succID, succAddr, err := ka.table.GetSuccessor()
		if err != nil {
			if errors.Is(err, routingtable.ErrNoSuccessor) {
				waitDuration = ka.keepAliveInterval // No successor, wait indefinitely
			}
		} else {
			lastSeen := time.Unix(0, ka.lastSeen.Load())
			// calculate the time elapsed since the last seen message
			elapsed := time.Since(lastSeen)
			if elapsed < ka.keepAliveInterval {
				// wait for the remaining time until the next keep-alive
				waitDuration = ka.keepAliveInterval - elapsed
			} else {
				// if the elapsed time is greater than the keep-alive interval, send a keep-alive message
				err := ka.net.SendKeepAlive(succAddr)
				if err != nil {
					logger.Log.Warnf("Successor %s failed to respond to keep-alive message: %v. IMPLEMENT FAILURE", succID.ToHexString(), err)
					//TODO: implement the failure handling procedure
				} else {
					now := time.Now().UnixNano()
					ka.lastSeen.Store(now)
				}
				// in this case, we wait for the keep-alive interval to avoid sending too many messages
				waitDuration = ka.keepAliveInterval
			}
		}

		// wait for the next iteration or until the context is done
		timer := time.NewTimer(waitDuration)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			timer.Stop()
		}
	}
}

// Stop stops the keep-alive goroutine and cleans up resources.
func (ka *KeepAlive) Stop() {
	if ka.cancel != nil {
		ka.cancel() // Cancel the context to stop the goroutine
		ka.cancel = nil
	}
	ka.table = nil // Clear the successor reference
	ka.net = nil   // Clear the network reference
}

// UpdateLastSeen updates the timestamp of the last message received from the successor.
func (ka *KeepAlive) UpdateLastSeen() {
	now := time.Now().UnixNano()
	ka.lastSeen.Store(now) // Update the last seen timestamp
}
