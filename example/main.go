package main

import (
	"fmt"
	"log"

	"github.com/KingrogKDR/kWALity"
)

func main() {
	wal, err := kWALity.Open("wal-dir")
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	wal.Close()
	fmt.Printf("%+v\n", wal)
}
