package mapreduce

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func runTest(files []string, reducers int, output string) ([]error, error) {
	pairs, fileErrors := StartMapWorkers(files)
	partitions := PartitionPairs(pairs, reducers)
	groups := ShufflePartitions(partitions)
	outcomes := StartReduceWorkers(groups)

	var results []Result
	var errorsFound []error

	for outcome := range outcomes {
		if outcome.Err != nil {
			errorsFound = append(errorsFound, outcome.Err)
			continue
		}

		results = append(results, outcome.Result)
	}

	for err := range fileErrors {
		errorsFound = append(errorsFound, err)
	}

	err := WriteOutput(output, results)
	return errorsFound, err
}

func TestWordCount(t *testing.T) {
	dir := t.TempDir()

	first := filepath.Join(dir, "input1.txt")
	second := filepath.Join(dir, "input2.txt")

	err := os.WriteFile(first, []byte("Go, go! Map."), 0600)
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(second, []byte("MAP reduce."), 0600)
	if err != nil {
		t.Fatal(err)
	}

	expected := "go: 2\nmap: 2\nreduce: 1\n"

	for _, reducers := range []int{1, 3, 5} {
		t.Run(fmt.Sprint(reducers), func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "output.txt")

			errs, err := runTest(
				[]string{first, second}, reducers, output,
			)

			if err != nil || len(errs) > 0 {
				t.Fatalf("errors: %v, %v", err, errs)
			}

			data, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}

			if string(data) != expected {
				t.Fatalf("got %q, want %q", data, expected)
			}
		})
	}
}

func TestMissingAndEmptyFiles(t *testing.T) {
	dir := t.TempDir()

	valid := filepath.Join(dir, "valid.txt")
	empty := filepath.Join(dir, "empty.txt")
	missing := filepath.Join(dir, "missing.txt")
	output := filepath.Join(dir, "output.txt")

	err := os.WriteFile(valid, []byte("The THE"), 0600)
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(empty, nil, 0600)
	if err != nil {
		t.Fatal(err)
	}

	errs, err := runTest(
		[]string{valid, empty, missing}, 3, output,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "missing.txt") {
		t.Fatalf("unexpected errors: %v", errs)
	}

	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != "the: 2\n" {
		t.Fatalf("unexpected output: %q", data)
	}
}
