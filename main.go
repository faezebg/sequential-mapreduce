package main

import (
	"flag"
	"fmt"
	"os"

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
		"Starting %d Map workers and %d Reduce workers...\n",
		len(inputFiles),
		*nReduce,
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

	// Phase 4: Reduce
	results := mapreduce.StartReduceWorkers(groups)

	// Consume reducer results.
	uniqueWords := 0
	totalOccurrences := 0

	for outcome := range results {
		if outcome.Err != nil {
			fmt.Fprintln(
				os.Stderr,
				"reduce error:",
				outcome.Err,
			)
			continue
		}

		uniqueWords++
		totalOccurrences += outcome.Result.Count
	}

	// Report file errors.
	for err := range errs {
		fmt.Fprintln(
			os.Stderr,
			"map error:",
			err,
		)
	}

	fmt.Printf(
		"Total unique words: %d\n",
		uniqueWords,
	)

	fmt.Printf(
		"Total word occurrences: %d\n",
		totalOccurrences,
	)

	fmt.Println(
		"Reduce phase complete; sorted output.txt is next.",
	)
}
