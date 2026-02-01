package main

import (
	"log"
	"github.com/aryand15/go-redis/app/storage"
	"github.com/aryand15/go-redis/app/commands"
	"github.com/aryand15/go-redis/app/server"
)

func main() {
	db := storage.NewDB() // make a fresh database
	handler := commands.NewCommandHandler(db) // make a CommandHandler instance to handle commands on this DB

	// Start the server, handling any errors
	if err := server.StartServer(handler); err != nil {
		log.Fatalf("Server error: %v\n", err)
	}
}