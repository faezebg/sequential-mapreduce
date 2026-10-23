package mapreduce

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteOutputSorted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output.txt")

	results := []Result{
		{Word: "map", Count: 2},
		{Word: "apple", Count: 3},
		{Word: "go", Count: 5},
	}

	if err := WriteOutput(path, results); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	want := "apple: 3\ngo: 5\nmap: 2\n"

	if string(data) != want {
		t.Fatalf(
			"output = %q, want %q",
			string(data),
			want,
		)
	}

	if results[0].Word != "map" {
		t.Fatal(
			"WriteOutput modified the original results",
		)
	}
}

func TestWriteOutputEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output.txt")

	if err := WriteOutput(path, nil); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) != 0 {
		t.Fatalf(
			"expected empty output, got %q",
			data,
		)
	}
}

func TestWriteOutputInvalidPath(t *testing.T) {
	path := filepath.Join(
		t.TempDir(),
		"missing-dir",
		"output.txt",
	)

	err := WriteOutput(
		path,
		[]Result{{Word: "go", Count: 1}},
	)

	if err == nil {
		t.Fatal(
			"expected error for invalid output path",
		)
	}
}
