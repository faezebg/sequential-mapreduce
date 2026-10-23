package mapreduce

import (
	"fmt"
	"strconv"
	"sync"
)

// Reduce adds the intermediate values for one word.
func Reduce(group Group) (Result, error) {
	sum := 0

	for _, value := range group.Values {
		number, err := strconv.Atoi(value)
		if err != nil {
			return Result{}, fmt.Errorf(
				"invalid value %q for word %q: %w",
				value, group.Key, err,
			)
		}

		sum += number
	}

	return Result{
		Word:  group.Key,
		Count: sum,
	}, nil
}

// ReduceOutcome carries either a result or an error.
type ReduceOutcome struct {
	Result Result
	Err    error
}

// StartReduceWorkers runs one goroutine per partition.
func StartReduceWorkers(
	partitions []<-chan Group,
) <-chan ReduceOutcome {

	results := make(chan ReduceOutcome, 256)

	var wg sync.WaitGroup

	for _, partition := range partitions {
		wg.Add(1)

		go func(groups <-chan Group) {
			defer wg.Done()

			for group := range groups {
				result, err := Reduce(group)

				results <- ReduceOutcome{
					Result: result,
					Err:    err,
				}
			}
		}(partition)
	}

	// Close results after all reducers finish.
	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}
