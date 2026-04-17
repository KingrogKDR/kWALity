package kWALity

import (
	"fmt"
	"log"
)

func main() {
	wal, err := OpenWal("write-ahead-log")
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	fmt.Printf("%+v\n", wal)
}
