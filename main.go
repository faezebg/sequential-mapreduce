package main

import (
	"flag"
	"fmt"
	"os"
	"sync"

	"example.com/sequential-mapreduce/mapreduce"
)

func main() {
	nReduce := flag.Int(
		"reducers",
		3,
		"number of reduce partitions",
	)

	flag.Parse()

	if *nReduce <= 0 {
		fmt.Fprintln(
			os.Stderr,
			"error: -reducers must be greater than zero",
		)
		os.Exit(1)
	}

	inputFiles := flag.Args()

	if len(inputFiles) == 0 {
		inputFiles = []string{
			"input1.txt",
			"input2.txt",
			"input3.txt",
			"input4.txt",
			"input5.txt",
		}
	}

	fmt.Printf(
		"Starting %d Map workers...\n",
		len(inputFiles),
	)

	// Phase 1: Map
	pairs, errs := mapreduce.StartMapWorkers(inputFiles)

	// Phase 2: Partition
	partitions := mapreduce.PartitionPairs(
		pairs,
		*nReduce,
	)

	// Temporary consumers.
	// Shuffle/Group will replace these later.
	counts := make([]int, len(partitions))

	var wg sync.WaitGroup

	for i, partition := range partitions {
		wg.Add(1)

		go func(
			index int,
			channel <-chan mapreduce.KeyValue,
		) {
			defer wg.Done()

			for range channel {
				counts[index]++
			}
		}(i, partition)
	}

	// Wait for all partition consumers.
	wg.Wait()

	total := 0

	for i, count := range counts {
		fmt.Printf(
			"Partition %d: %d pairs\n",
			i,
			count,
		)
		total += count
	}

	// Report any Map errors.
	for err := range errs {
		fmt.Fprintln(os.Stderr, "map error:", err)
	}

	fmt.Printf(
		"Total intermediate pairs: %d\n",
		total,
	)

	fmt.Println(
		"Partition phase complete; Shuffle and Reduce are next.",
	)
}
