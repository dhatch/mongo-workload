package main

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"
)

// The address of the primary server to connect to.
const PRIMARY = "127.0.0.1:30000"

// Simulate NUM_CLIENTS
const NUM_CLIENTS = 100

// Workload parameters
const WRITE_PERCENTAGE = 0.60 // Percent of operations that are writes

// Write parameters
//
// Must sum to < 1
const UPDATE_PERCENTAGE = 0.10 // Percent of write operations that are updates
const DELETE_PERCENTAGE = 0.10 // Percent of write operations that are delete

type TestData struct {
	Number int64
	Group  int64
}

type Stats struct {
	emptyReads         uint64
	totalReads         uint64
	totalDocumentsRead uint64
	totalWrites        uint64
}

// The function simulate simulates a workload for a single client.
// It uses the workload parameter constants to specify the workload.
//
// Documents will be inserted in the form:
// { group: Long , number: Long }
//
// where number is a unique identifier.
func simulate(group int64, nextNumber *int64, stats *Stats, ch chan int) {
	session, err := mgo.Dial(PRIMARY)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	for {
		rand_source := rand.New(rand.NewSource(time.Now().UnixNano()))

		c := session.DB("test").C("workload")
		number := rand_source.Int63n(atomic.LoadInt64(nextNumber))
		if rand_source.Float32() <= WRITE_PERCENTAGE {
			insert_type := rand_source.Float32()
			if insert_type <= DELETE_PERCENTAGE {
				// Perform a
				atomic.AddUint64(&(stats.totalReads), 1)
				number := rand_source.Int63n(atomic.LoadInt64(nextNumber))
				query := c.Find(bson.M{"number": number}).Limit(1)
				count, err := query.Count()
				if err != nil {
					panic(err)
				}

				if count == 0 {
					atomic.AddUint64(&(stats.emptyReads), 1)
					log.Print("empty read")
				} else {
					log.Print("read", count)
				}
				atomic.AddUint64(&(stats.totalDocumentsRead), uint64(count))
				query.Iter()
			} else if insert_type <= UPDATE_PERCENTAGE+DELETE_PERCENTAGE {

			} else {
				// Perform an insertion
				atomic.AddInt64(nextNumber, 1)
				atomic.AddUint64(&(stats.totalWrites), 1)
				err = c.Insert(&TestData{number, group})
				if err != nil {
					log.Fatal(err)
				}
			}
		} else {
			// Perform a read
			atomic.AddUint64(&(stats.totalReads), 1)
			number := rand_source.Int63n(atomic.LoadInt64(nextNumber))
			query := c.Find(bson.M{"number": number}).Limit(1)
			count, err := query.Count()
			if err != nil {
				panic(err)
			}

			if count == 0 {
				atomic.AddUint64(&(stats.emptyReads), 1)
				log.Print("empty read")
			} else {
				log.Print("read", count)
			}
			atomic.AddUint64(&(stats.totalDocumentsRead), uint64(count))
			query.Iter()
		}

		runtime.Gosched()
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	c := make(chan int, runtime.NumCPU())
	stats := Stats{0, 0, 0, 0}

	nextNumber := int64(1000)
	for i := int64(0); i < NUM_CLIENTS; i++ {
		fmt.Println("Spawn inserter", i)
		go simulate(i, &nextNumber, &stats, c)
	}

	// Wait for interrupt signal
	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	// Print out statistics
	fmt.Println()
	fmt.Println("Write percentage: ", float64(stats.totalWrites)/float64(stats.totalWrites+stats.totalReads))
	fmt.Println("Empty read percentage: ", float64(stats.emptyReads)/float64(stats.totalReads))
	fmt.Println("Total documents read: ", stats.totalDocumentsRead)
	fmt.Println("Total documents written: ", stats.totalWrites)
}
