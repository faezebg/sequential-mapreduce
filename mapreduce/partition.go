package mapreduce

import "hash/fnv"

// Partition determines which reducer partition
// should receive a given word.
func Partition(word string, nReduce int) int {
	if nReduce <= 0 {
		panic("nReduce must be greater than zero")
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(word))

	return int(uint64(h.Sum32()) % uint64(nReduce))
}

// PartitionPairs sends intermediate pairs
// to their corresponding partition channels.
func PartitionPairs(
	input <-chan KeyValue,
	nReduce int,
) []<-chan KeyValue {

	if nReduce <= 0 {
		panic("nReduce must be greater than zero")
	}

	outputs := make([]chan KeyValue, nReduce)
	receiveOnly := make([]<-chan KeyValue, nReduce)

	// Create a separate channel for each partition.
	for i := 0; i < nReduce; i++ {
		outputs[i] = make(chan KeyValue, 256)
		receiveOnly[i] = outputs[i]
	}

	// Route pairs to their partitions.
	go func() {
		defer func() {
			for _, output := range outputs {
				close(output)
			}
		}()

		for pair := range input {
			id := Partition(pair.Key, nReduce)
			outputs[id] <- pair
		}
	}()

	return receiveOnly
}
