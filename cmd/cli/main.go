package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "GuptaDHT/api/gen/client"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	serverAddress = "localhost:50051" // cambia se serve
	routingFile   = "routingtable.txt"
	chunkSize     = 32 * 1024
)

func main() {
	// Connessione al server gRPC
	conn, err := grpc.NewClient(serverAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewClientServiceClient(conn)

	fmt.Println("Simple gRPC Client – type 'help' for commands")
	reader := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !reader.Scan() {
			break // EOF (Ctrl‑D)
		}
		line := strings.TrimSpace(reader.Text())
		if line == "" {
			continue
		}

		args := strings.Fields(line)
		cmd := strings.ToLower(args[0])

		switch cmd {
		case "help":
			printHelp()
		case "quit", "exit":
			fmt.Println("Bye!")
			return
		case "getroutingtable":
			getRoutingTable(client)
		case "getresource":
			if len(args) < 2 {
				fmt.Println("Usage: getresource <filename>")
				continue
			}
			getResource(client, args[1])
		case "setresource":
			if len(args) < 2 {
				fmt.Println("Usage: setresource <filepath>")
				continue
			}
			setResource(client, args[1])
		default:
			fmt.Println("Unknown command – type 'help' to list commands")
		}
	}

	if err := reader.Err(); err != nil {
		log.Println("Input error:", err)
	}
}

// printHelp shows available commands
func printHelp() {
	fmt.Println(`Available commands:
  getroutingtable           – write routing table to routingtable.txt
  getresource <filename>    – download a resource to current folder
  setresource <filepath>    – upload a local file
  help                      – show this help
  quit / exit               – leave the client`)
}

// getRoutingTable saves the routing table to routingtable.txt
func getRoutingTable(client pb.ClientServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetRoutingTable(ctx, &emptypb.Empty{})
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	f, err := os.Create(routingFile)
	if err != nil {
		fmt.Println("File error:", err)
		return
	}
	defer f.Close()

	for _, e := range resp.Entries {
		fmt.Fprintf(f, "NodeID:%s Address:%s Super:%t SliceLeader:%t UnitLeader:%t\n",
			e.NodeId, e.Address, e.IsSupernode, e.IsSliceLeader, e.IsUnitLeader)
	}
	fmt.Println("Routing table written to", routingFile)
}

// getResource downloads a file from the server
func getResource(client pb.ClientServiceClient, filename string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	stream, err := client.GetResource(ctx, &pb.RequestResource{Filename: filename})
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	out, err := os.Create(filename)
	if err != nil {
		fmt.Println("File error:", err)
		return
	}
	defer out.Close()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("Download complete:", filename)
			return
		}
		if err != nil {
			fmt.Println("Stream error:", err)
			return
		}
		if _, err = out.Write(chunk.Data); err != nil {
			fmt.Println("Write error:", err)
			return
		}
	}
}

// setResource uploads a local file to the server
func setResource(client pb.ClientServiceClient, path string) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Open error:", err)
		return
	}
	defer file.Close()

	stream, err := client.StoreResource(context.Background())
	if err != nil {
		fmt.Println("Stream error:", err)
		return
	}

	buf := make([]byte, chunkSize)
	var offset uint64
	filename := filepath.Base(path)

	for {
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Read error:", err)
			return
		}

		if err = stream.Send(&pb.ResourceResponse{
			Filename: filename,
			Offset:   offset,
			Data:     buf[:n],
			Eof:      false,
		}); err != nil {
			fmt.Println("Send error:", err)
			return
		}
		offset += uint64(n)
	}

	// final empty chunk with EOF flag
	if err = stream.Send(&pb.ResourceResponse{
		Filename: filename,
		Offset:   offset,
		Eof:      true,
	}); err != nil {
		fmt.Println("Send error:", err)
		return
	}

	if _, err = stream.CloseAndRecv(); err != nil {
		fmt.Println("Close error:", err)
		return
	}
	fmt.Println("Upload complete:", filename)
}
