package main

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"log"
	"runtime"
	"sync/atomic"
)

// The address of the primary server to connect to.
const PRIMARY = "127.0.0.1:30000"

// Simulate NUM_CLIENTS
const NUM_CLIENTS = 1000

// Workload parameters
const WRITE_PERCENTAGE = 0.60  // Percent of operations that are writes
const UPDATE_PERCENTAGE = 0.10 // Percent of operations that are updates

type TestData struct {
	number uint64
	group  uint64
}

// The function simulate simulates a workload for a single client.
// It uses the workload parameter constants to specify the workload.
//
// Documents will be inserted in the form:
// { group: Long , number: Long }
//
// where number is a unique identifier
// Updates will target a specific document.
func simulate(group uint64, numberDocs uint64, ch chan int) {
	session, err := mgo.Dial(PRIMARY)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c := session.DB("test").C("workload")
	for i := uint64(0); i < numberDocs; i++ {
		err = c.Insert(&TestData{i, group})
	}

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Done inserting", group)
	ch <- 1
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Println("Begin insertion")
	c := make(chan int, NUM_INSERTERS)
	for i := uint64(0); i < NUM_INSERTERS; i++ {
		fmt.Println("Spawn inserter", i)
		go insert(i, NUM_DOCS, c)
	}

	for i := 0; i < NUM_INSERTERS; i++ {
		<-c
	}
}
