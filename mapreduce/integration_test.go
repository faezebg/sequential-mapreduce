package mapreduce

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Execute the complete MapReduce pipeline for tests.
func runPipelineForTest(
	files []string,
	nReduce int,
) ([]Result, []error) {

	pairs, mapErrors := StartMapWorkers(files)

	partitions := PartitionPairs(pairs, nReduce)

	groups := ShufflePartitions(partitions)

	outcomes := StartReduceWorkers(groups)

	var results []Result
	var errorsFound []error

	// Read all results to avoid blocking workers.
	for outcome := range outcomes {
		if outcome.Err != nil {
			errorsFound = append(
				errorsFound,
				outcome.Err,
			)
			continue
		}

		results = append(results, outcome.Result)
	}

	// Collect file errors after all results.
	for err := range mapErrors {
		errorsFound = append(errorsFound, err)
	}

	return results, errorsFound
}

// Test the complete pipeline using the assignment example.
func TestEndToEndWordCount(t *testing.T) {
	dir := t.TempDir()

	first := filepath.Join(dir, "input1.txt")
	second := filepath.Join(dir, "input2.txt")

	err := os.WriteFile(
		first,
		[]byte("the quick brown fox jumps over the lazy dog"),
		0600,
	)
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(
		second,
		[]byte("the dog barks and the fox runs away"),
		0600,
	)
	if err != nil {
		t.Fatal(err)
	}

	expected := "and: 1\n" +
		"away: 1\n" +
		"barks: 1\n" +
		"brown: 1\n" +
		"dog: 2\n" +
		"fox: 2\n" +
		"jumps: 1\n" +
		"lazy: 1\n" +
		"over: 1\n" +
		"quick: 1\n" +
		"runs: 1\n" +
		"the: 4\n"

	// The answer must not depend on reducer count.
	for _, reducers := range []int{1, 3, 5} {
		t.Run(
			fmt.Sprintf("reducers_%d", reducers),
			func(t *testing.T) {
				results, errs := runPipelineForTest(
					[]string{first, second},
					reducers,
				)

				if len(errs) != 0 {
					t.Fatalf(
						"unexpected errors: %v",
						errs,
					)
				}

				output := filepath.Join(
					t.TempDir(),
					"output.txt",
				)

				if err := WriteOutput(output, results); err != nil {
					t.Fatal(err)
				}

				got, err := os.ReadFile(output)
				if err != nil {
					t.Fatal(err)
				}

				if string(got) != expected {
					t.Fatalf(
						"unexpected output:\n%s\nwant:\n%s",
						got,
						expected,
					)
				}
			},
		)
	}
}

// A missing file must not stop valid files.
func TestEndToEndMissingFile(t *testing.T) {
	dir := t.TempDir()

	valid := filepath.Join(dir, "valid.txt")
	missing := filepath.Join(dir, "missing.txt")

	err := os.WriteFile(
		valid,
		[]byte("Hello, HELLO! Go."),
		0600,
	)
	if err != nil {
		t.Fatal(err)
	}

	results, errs := runPipelineForTest(
		[]string{missing, valid},
		3,
	)

	if len(errs) != 1 ||
		!strings.Contains(errs[0].Error(), "missing.txt") {
		t.Fatalf(
			"expected one missing-file error, got %v",
			errs,
		)
	}

	output := filepath.Join(
		t.TempDir(),
		"output.txt",
	)

	if err := WriteOutput(output, results); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}

	expected := "go: 1\nhello: 2\n"

	if string(data) != expected {
		t.Fatalf(
			"valid file was not fully processed: %q",
			data,
		)
	}
}

// Empty files should produce no words or errors.
func TestEndToEndEmptyFile(t *testing.T) {
	input := filepath.Join(
		t.TempDir(),
		"empty.txt",
	)

	if err := os.WriteFile(input, nil, 0600); err != nil {
		t.Fatal(err)
	}

	results, errs := runPipelineForTest(
		[]string{input},
		3,
	)

	if len(errs) != 0 {
		t.Fatalf(
			"unexpected errors: %v",
			errs,
		)
	}

	if len(results) != 0 {
		t.Fatalf(
			"expected no results, got %v",
			results,
		)
	}
}
