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
		"Reduce partitions (for later steps): %d\n",
		*nReduce,
	)

	fmt.Printf(
		"Starting %d map workers...\n",
		len(inputFiles),
	)

	// Start concurrent Map workers
	pairs, errs := mapreduce.StartMapWorkers(inputFiles)

	// Consume intermediate pairs
	pairCount := 0

	for range pairs {
		pairCount++
	}

	// Report file errors
	for err := range errs {
		fmt.Fprintln(os.Stderr, "map error:", err)
	}

	fmt.Printf(
		"Intermediate pairs generated: %d\n",
		pairCount,
	)

	fmt.Println(
		"Map phase complete; Shuffle and Reduce are next.",
	)
}
