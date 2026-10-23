package mapreduce

import "sort"

// ShufflePartition groups all values
// belonging to the same key.
func ShufflePartition(input <-chan KeyValue) <-chan Group {
	output := make(chan Group)

	go func() {
		defer close(output)

		// Store values for each key.
		grouped := make(map[string][]string)

		for pair := range input {
			grouped[pair.Key] = append(
				grouped[pair.Key],
				pair.Value,
			)
		}

		// Sort keys for deterministic output.
		keys := make([]string, 0, len(grouped))

		for key := range grouped {
			keys = append(keys, key)
		}

		sort.Strings(keys)

		// Send groups to the next stage.
		for _, key := range keys {
			output <- Group{
				Key:    key,
				Values: grouped[key],
			}
		}
	}()

	return output
}

// ShufflePartitions starts one grouping
// goroutine for each partition.
func ShufflePartitions(
	partitions []<-chan KeyValue,
) []<-chan Group {

	outputs := make([]<-chan Group, len(partitions))

	for i, partition := range partitions {
		outputs[i] = ShufflePartition(partition)
	}

	return outputs
}
