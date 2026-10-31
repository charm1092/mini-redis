package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Server struct {
	Address      string
	Listener     net.Listener
	messagesChan chan Message
	clients      map[net.Conn]*Peer
	mu           sync.RWMutex
	deadClients  []net.Conn
}

func NewServer(address string) *Server {
	return &Server{
		Address:      address,
		messagesChan: make(chan Message, 100),
		clients:      make(map[net.Conn]*Peer),
	}
}

type Message struct {
	Author string
	Text   string
}

type Peer struct {
	Conn        net.Conn
	ConnectedAt time.Time
}

func (s *Server) acceptLoop(ctx context.Context) {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				log.Println("Accept loop stopped")
				return
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}
		log.Printf("Welcome, %s", conn.RemoteAddr().String())
		s.registerPeer(conn)
		go s.handleConn(conn)
	}
}

func (s *Server) Start() error {
	var err error
	s.Listener, err = net.Listen("tcp", s.Address)
	if err != nil {
		log.Printf("Error accept: %v", err)
		return err
	}

	log.Printf("Server started")
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	defer s.Listener.Close()

	go s.acceptLoop(ctx)
	go s.Broadcast(ctx)
	s.Stop(ctx)

	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 2048)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("Connection error: %v", err)
			return
		}

		msg := &Message{
			Author: conn.RemoteAddr().String(),
			Text:   string(buf[:n]),
		}
		s.messagesChan <- *msg
	}
}

func (s *Server) registerPeer(conn net.Conn) {
	peer := &Peer{
		Conn:        conn,
		ConnectedAt: time.Now(),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[conn] = peer
}

func (s *Server) Broadcast(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-s.messagesChan:
			s.mu.RLock()
			message := fmt.Sprintf("%s: %s\n", msg.Author, msg.Text)
			for _, client := range s.clients {
				s.writeInConnection(client.Conn, message)
			}
			s.mu.RUnlock()

			for _, conn := range s.deadClients {
				s.unregisterPeer(conn)
			}
			s.deadClients = s.deadClients[:0]
		}

	}
}

func (s *Server) writeInConnection(conn net.Conn, message string) {
	_, err := conn.Write([]byte(message))
	if err != nil {
		log.Printf("Failed write message: %v", err)
		s.deadClients = append(s.deadClients, conn)
	}
}

func (s *Server) unregisterPeer(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	conn.Close()
	delete(s.clients, conn)
	log.Printf("Client disconnected: %s", conn.RemoteAddr().String())
}

func (s *Server) Stop(ctx context.Context) {
	<-ctx.Done()
	s.mu.RLock()
	connections := make([]net.Conn, 0, len(s.clients))
	for _, client := range s.clients {
		connections = append(connections, client.Conn)
	}
	s.mu.RUnlock()

	for _, conn := range connections {
		s.unregisterPeer(conn)
	}
}
