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

	// Phase 3: Shuffle / Group
	groups := mapreduce.ShufflePartitions(partitions)

	// Temporary consumers.
	// Reduce workers will replace these later.
	pairCounts := make([]int, len(groups))
	groupCounts := make([]int, len(groups))

	var wg sync.WaitGroup

	for i, groupChannel := range groups {
		wg.Add(1)

		go func(
			index int,
			channel <-chan mapreduce.Group,
		) {
			defer wg.Done()

			for group := range channel {
				groupCounts[index]++
				pairCounts[index] += len(group.Values)
			}
		}(i, groupChannel)
	}

	wg.Wait()

	totalPairs := 0
	totalGroups := 0

	for i := range groups {
		fmt.Printf(
			"Partition %d: %d pairs, %d groups\n",
			i,
			pairCounts[i],
			groupCounts[i],
		)

		totalPairs += pairCounts[i]
		totalGroups += groupCounts[i]
	}

	// Report Map errors.
	for err := range errs {
		fmt.Fprintln(os.Stderr, "map error:", err)
	}

	fmt.Printf(
		"Total intermediate pairs: %d\n",
		totalPairs,
	)

	fmt.Printf(
		"Total unique words: %d\n",
		totalGroups,
	)

	fmt.Println(
		"Shuffle/Group complete; Reduce is next.",
	)
}
