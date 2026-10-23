package mapreduce

import (
	"reflect"
	"testing"
)

func TestShufflePartition(t *testing.T) {
	input := make(chan KeyValue, 5)

	input <- KeyValue{Key: "go", Value: "1"}
	input <- KeyValue{Key: "map", Value: "1"}
	input <- KeyValue{Key: "go", Value: "1"}
	input <- KeyValue{Key: "go", Value: "2"}
	input <- KeyValue{Key: "map", Value: "1"}

	close(input)

	var got []Group

	for group := range ShufflePartition(input) {
		got = append(got, group)
	}

	want := []Group{
		{
			Key:    "go",
			Values: []string{"1", "1", "2"},
		},
		{
			Key:    "map",
			Values: []string{"1", "1"},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf(
			"ShufflePartition() = %v; want %v",
			got, want,
		)
	}
}

func TestShufflePartitionsAfterPartition(t *testing.T) {
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

	const nReduce = 3

	partitions := PartitionPairs(input, nReduce)
	outputs := ShufflePartitions(partitions)

	got := make(map[string][]string)

	for id, channel := range outputs {
		for group := range channel {
			if Partition(group.Key, nReduce) != id {
				t.Errorf(
					"key %q arrived in wrong partition %d",
					group.Key, id,
				)
			}

			if _, exists := got[group.Key]; exists {
				t.Errorf(
					"duplicate group for key %q",
					group.Key,
				)
			}

			got[group.Key] = group.Values
		}
	}

	want := map[string][]string{
		"go":     {"1", "1"},
		"map":    {"1", "1"},
		"reduce": {"1"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf(
			"ShufflePartitions() = %v; want %v",
			got, want,
		)
	}
}

func TestShuffleEmptyPartition(t *testing.T) {
	input := make(chan KeyValue)
	close(input)

	for group := range ShufflePartition(input) {
		t.Fatalf(
			"expected no groups, got %v",
			group,
		)
	}
}
