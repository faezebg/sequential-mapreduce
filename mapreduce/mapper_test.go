package mapreduce

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMap(t *testing.T) {
	out := make(chan KeyValue, 8)

	Map("Go, GO! map-reduce", out)
	close(out)

	var got []KeyValue

	for pair := range out {
		got = append(got, pair)
	}

	want := []KeyValue{
		{Key: "go", Value: "1"},
		{Key: "go", Value: "1"},
		{Key: "map", Value: "1"},
		{Key: "reduce", Value: "1"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Map() = %v, want %v", got, want)
	}
}

func TestWorkersContinueOnMissingFile(t *testing.T) {
	dir := t.TempDir()

	good := filepath.Join(dir, "exists.txt")
	missing := filepath.Join(dir, "missing.txt")

	if err := os.WriteFile(
		good,
		[]byte("Hello hello"),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	pairs, errs := StartMapWorkers(
		[]string{good, missing},
	)

	count := 0

	for range pairs {
		count++
	}

	errorCount := 0

	for range errs {
		errorCount++
	}

	if count != 2 || errorCount != 1 {
		t.Fatalf(
			"pairs=%d, errors=%d; want pairs=2, errors=1",
			count,
			errorCount,
		)
	}
}
