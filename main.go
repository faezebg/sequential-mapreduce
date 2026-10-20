package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	// Number of Reduce partitions
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

	// Read input filenames from command line
	inputFiles := flag.Args()

	// Use default files if no filenames are provided
	if len(inputFiles) == 0 {
		inputFiles = []string{
			"input1.txt",
			"input2.txt",
			"input3.txt",
			"input4.txt",
			"input5.txt",
		}
	}

	fmt.Printf("Reduce partitions: %d\n", *nReduce)
	fmt.Printf("Input files (%d):\n", len(inputFiles))

	for _, filename := range inputFiles {
		fmt.Println(" -", filename)
	}
}
