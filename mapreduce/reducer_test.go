package mapreduce

import (
	"reflect"
	"strings"
	"testing"
)

func TestReduce(t *testing.T) {
	group := Group{
		Key:    "go",
		Values: []string{"1", "2", "3"},
	}

	got, err := Reduce(group)
	if err != nil {
		t.Fatal(err)
	}

	want := Result{
		Word:  "go",
		Count: 6,
	}

	if got != want {
		t.Fatalf(
			"Reduce() = %+v, want %+v",
			got, want,
		)
	}
}

func TestReduceInvalidValue(t *testing.T) {
	group := Group{
		Key:    "go",
		Values: []string{"1", "invalid"},
	}

	_, err := Reduce(group)

	if err == nil || !strings.Contains(
		err.Error(), "invalid",
	) {
		t.Fatalf(
			"expected invalid value error, got %v",
			err,
		)
	}
}

func TestReducePipeline(t *testing.T) {
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

	partitions := PartitionPairs(input, 3)
	groups := ShufflePartitions(partitions)
	results := StartReduceWorkers(groups)

	got := make(map[string]int)

	for outcome := range results {
		if outcome.Err != nil {
			t.Fatal(outcome.Err)
		}

		word := outcome.Result.Word

		if _, exists := got[word]; exists {
			t.Fatalf(
				"duplicate result for word %q",
				word,
			)
		}

		got[word] = outcome.Result.Count
	}

	want := map[string]int{
		"go":     2,
		"map":    2,
		"reduce": 1,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf(
			"got %v, want %v",
			got, want,
		)
	}
}

func TestReduceEmptyPartitions(t *testing.T) {
	partitions := make([]<-chan Group, 3)

	for i := range partitions {
		ch := make(chan Group)
		close(ch)
		partitions[i] = ch
	}

	for outcome := range StartReduceWorkers(partitions) {
		t.Fatalf(
			"unexpected result: %+v",
			outcome,
		)
	}
}
