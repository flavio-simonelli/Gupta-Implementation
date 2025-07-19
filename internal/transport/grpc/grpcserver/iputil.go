package grpcserver

import (
	"net"
)

// getLocalIP attempts to determine the local IP by creating a dummy UDP connection
func getLocalIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80") // Google's DNS
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String(), nil
}
