package mapreduce

import (
	"reflect"
	"sync"
	"testing"
)

func TestPartition(t *testing.T) {
	words := []string{
		"go",
		"map",
		"reduce",
		"go",
		"hello",
	}

	for _, nReduce := range []int{1, 2, 3, 5} {
		for _, word := range words {
			got := Partition(word, nReduce)

			if got < 0 || got >= nReduce {
				t.Fatalf(
					"Partition(%q, %d) = %d, outside range",
					word, nReduce, got,
				)
			}

			again := Partition(word, nReduce)

			if got != again {
				t.Fatalf(
					"Partition changed: %d vs %d",
					got, again,
				)
			}
		}
	}
}

func TestPartitionPairs(t *testing.T) {
	pairs := []KeyValue{
		{Key: "go", Value: "1"},
		{Key: "map", Value: "1"},
		{Key: "go", Value: "1"},
		{Key: "reduce", Value: "1"},
		{Key: "map", Value: "1"},
	}

	input := make(chan KeyValue, len(pairs))

	for _, pair := range pairs {
		input <- pair
	}
	close(input)

	outputs := PartitionPairs(input, 3)

	received := make([][]KeyValue, len(outputs))

	var wg sync.WaitGroup

	for i, output := range outputs {
		wg.Add(1)

		go func(
			index int,
			channel <-chan KeyValue,
		) {
			defer wg.Done()

			for pair := range channel {
				received[index] = append(
					received[index],
					pair,
				)
			}
		}(i, output)
	}

	wg.Wait()

	want := map[string]int{
		"go":     2,
		"map":    2,
		"reduce": 1,
	}

	got := make(map[string]int)

	for index, items := range received {
		for _, pair := range items {
			if Partition(pair.Key, 3) != index {
				t.Fatalf(
					"word %q in wrong partition %d",
					pair.Key, index,
				)
			}

			if pair.Value != "1" {
				t.Fatalf(
					"unexpected value %q",
					pair.Value,
				)
			}

			got[pair.Key]++
		}
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf(
			"received %v; want %v",
			got, want,
		)
	}
}
