package main

import (
	"log"
	"github.com/aryand15/go-redis/app/storage"
	"github.com/aryand15/go-redis/app/commands"
	"github.com/aryand15/go-redis/app/server"
)

func main() {
	db := storage.NewDB()
	handler := commands.NewCommandHandler(db)

	if err := server.StartServer(handler); err != nil {
		log.Fatalf("Server error: %v\n", err)
	}
}