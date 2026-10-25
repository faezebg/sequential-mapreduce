package main

import (
	"flag"
	"fmt"
	"os"

	"example.com/sequential-mapreduce/mapreduce"
)

func main() {
	nReduce := flag.Int("reducers", 3, "number of reduce partitions")
	flag.Parse()
	if *nReduce <= 0 {
		fmt.Fprintln(os.Stderr, "error: -reducers must be greater than zero")
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
	fmt.Printf("Starting %d Map workers and %d Reduce workers...\n",
		len(inputFiles), *nReduce)

	pairs, errs := mapreduce.StartMapWorkers(inputFiles)
	partitions := mapreduce.PartitionPairs(pairs, *nReduce)
	groups := mapreduce.ShufflePartitions(partitions)
	outcomes := mapreduce.StartReduceWorkers(groups)

	var results []mapreduce.Result
	totalOccurrences := 0
	reduceFailed := false
	for outcome := range outcomes {
		if outcome.Err != nil {
			fmt.Fprintln(os.Stderr, "reduce error:", outcome.Err)
			reduceFailed = true
			continue
		}

		results = append(results, outcome.Result)
		totalOccurrences += outcome.Result.Count
	}

	for err := range errs {
		fmt.Fprintln(os.Stderr, "map error:", err)
	}
	if reduceFailed {
		fmt.Fprintln(os.Stderr, "output not written because Reduce failed")
		os.Exit(1)
	}
	err := mapreduce.WriteOutput("output.txt", results)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Total unique words: %d\n", len(results))
	fmt.Printf("Total word occurrences: %d\n", totalOccurrences)
	fmt.Println("Results saved to output.txt")
}
