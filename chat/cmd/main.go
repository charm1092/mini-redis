package main

import (
	"mini-redis/chat/server"
	// "server"
)

func main() {
	server := server.NewServer(":3000")
	server.Start()

	
}