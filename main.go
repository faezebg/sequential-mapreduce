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
	outcomes := mapreduce.StartReduceWorkers(groups)

	// Collect all reducer results.
	results := make([]mapreduce.Result, 0)

	totalOccurrences := 0
	reduceFailed := false

	for outcome := range outcomes {
		if outcome.Err != nil {
			fmt.Fprintln(
				os.Stderr,
				"reduce error:",
				outcome.Err,
			)
			reduceFailed = true
			continue
		}

		results = append(results, outcome.Result)
		totalOccurrences += outcome.Result.Count
	}

	// Report input file errors.
	for err := range errs {
		fmt.Fprintln(
			os.Stderr,
			"map error:",
			err,
		)
	}

	if reduceFailed {
		fmt.Fprintln(
			os.Stderr,
			"output not written because Reduce failed",
		)
		os.Exit(1)
	}

	// Phase 5: Sort and write output.
	if err := mapreduce.WriteOutput(
		"output.txt",
		results,
	); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf(
		"Total unique words: %d\n",
		len(results),
	)

	fmt.Printf(
		"Total word occurrences: %d\n",
		totalOccurrences,
	)

	fmt.Println("Results saved to output.txt")
}
