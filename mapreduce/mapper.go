package mapreduce

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"unicode"
)

func Map(text string, out chan<- KeyValue) {
	words := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	for _, word := range words {
		out <- KeyValue{Key: word, Value: "1"}
	}
}

func MapFile(filename string, out chan<- KeyValue) error {
	contents, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("read %q: %w", filename, err)
	}
	Map(string(contents), out)
	return nil
}

func StartMapWorkers(files []string) (<-chan KeyValue, <-chan error) {
	pairs := make(chan KeyValue, 256)
	errs := make(chan error, len(files))
	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()

			if err := MapFile(filename, pairs); err != nil {
				errs <- err
			}
		}(file)
	}
	go func() {
		wg.Wait()
		close(pairs)
		close(errs)
	}()

	return pairs, errs
}
