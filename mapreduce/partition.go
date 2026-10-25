package mapreduce

import "hash/fnv"

func Partition(word string, nReduce int) int {
	if nReduce <= 0 {
		panic("nReduce must be greater than zero")
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(word))
	hashValue := uint64(h.Sum32())
	return int(hashValue % uint64(nReduce))
}

func PartitionPairs(input <-chan KeyValue, nReduce int) []<-chan KeyValue {
	if nReduce <= 0 {
		panic("nReduce must be greater than zero")
	}
	channels := make([]chan KeyValue, nReduce)
	outputs := make([]<-chan KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		channels[i] = make(chan KeyValue, 256)
		outputs[i] = channels[i]
	}
	go func() {
		for pair := range input {
			id := Partition(pair.Key, nReduce)
			channels[id] <- pair
		}
		for _, ch := range channels {
			close(ch)
		}
	}()
	return outputs
}
