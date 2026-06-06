package main

import (
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// BufferPool implements memory reclamation by reusing byte slices via sync.Pool.
// This prevents heap fragmentation and reduces GC overhead under high-frequency streaming.
var bufferPool = sync.Pool{
	New: func() interface{} {
		// Allocate 32KB buffer (standard chunk size)
		return make([]byte, 32*1024)
	},
}

// FlowControlledCopy copies data from src to dst with flow control (backpressure) and memory reclamation.
func FlowControlledCopy(dst net.Conn, src net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()

	// Eagerly acquire a buffer from the pool
	buf := bufferPool.Get().([]byte)
	// Ensure the buffer is returned to the pool upon stream termination
	defer bufferPool.Put(buf)

	for {
		// Read from source (respecting flow control: we only read when ready to write)
		src.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := src.Read(buf)
		if n > 0 {
			// Write to destination (propagating backpressure: if dst is slow, Write blocks, stopping Read from src)
			dst.SetWriteDeadline(time.Now().Add(30 * time.Second))
			_, wErr := dst.Write(buf[:n])
			if wErr != nil {
				log.Printf("Write error (backpressure triggered or connection closed): %v", wErr)
				break
			}
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			break
		}
	}

	// Eagerly close connections to prevent resource leaks
	dst.Close()
	src.Close()
}

// HandleStream handles a long-lived gRPC/TCP stream connection.
func HandleStream(downstream net.Conn, upstreamAddr string) { 
	defer downstream.Close()

	// Connect to upstream gRPC/TCP server
	upstream, err := net.DialTimeout("tcp", upstreamAddr, 5*time.Second)
	if err != nil {
		log.Printf("Failed to connect to upstream: %v", err)
		return
	}
	defer upstream.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	// Downstream -> Upstream
	go FlowControlledCopy(upstream, downstream, &wg)
	// Upstream -> Downstream
	go FlowControlledCopy(downstream, upstream, &wg)

	wg.Wait()
}

func main() {
	log.Println("Starting Flow-Controlled gRPC/TCP Proxy...")
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go HandleStream(conn, "localhost:8081")
	}
}