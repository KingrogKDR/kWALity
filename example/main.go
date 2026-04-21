package main

import (
	"fmt"
	"log"

	"github.com/KingrogKDR/kWALity"
)

func main() {
	walOpts := kWALity.WalOpts{
		DirPath: "write-ahead-log",
	}
	wal, err := kWALity.Open(walOpts)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	fmt.Printf("%+v\n", wal)
}
