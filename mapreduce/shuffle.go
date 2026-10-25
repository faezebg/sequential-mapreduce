package mapreduce

import "sort"

func ShufflePartition(input <-chan KeyValue) <-chan Group {
	output := make(chan Group)
	go func() {
		defer close(output)
		grouped := make(map[string][]string)
		for pair := range input {
			grouped[pair.Key] = append(grouped[pair.Key], pair.Value)
		}
		var keys []string
		for key := range grouped {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			output <- Group{
				Key:    key,
				Values: grouped[key],
			}
		}
	}()
	return output
}

func ShufflePartitions(partitions []<-chan KeyValue) []<-chan Group {
	outputs := make([]<-chan Group, len(partitions))
	for i, partition := range partitions {
		outputs[i] = ShufflePartition(partition)
	}
	return outputs
}
