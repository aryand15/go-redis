package main

import (
	"fmt"
	"os"
	"github.com/aryand15/go-redis/app/storage"
	"github.com/aryand15/go-redis/app/commands"
	"github.com/aryand15/go-redis/app/server"
)

func main() {
	db := storage.NewDB()
	handler := commands.NewCommandHandler(db)

	if err := server.StartServer(handler); err != nil {
		fmt.Printf("Server error: %v\n", err)
		os.Exit(1)
	}
}