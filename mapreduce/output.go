package mapreduce

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

func WriteOutput(filename string, results []Result) error {
	sorted := make([]Result, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Word < sorted[j].Word
	})
	var output strings.Builder
	for _, result := range sorted {
		fmt.Fprintf(&output, "%s: %d\n", result.Word, result.Count)
	}

	err := os.WriteFile(filename, []byte(output.String()), 0644)
	if err != nil {
		return fmt.Errorf("cannot write output file %q: %w", filename, err)
	}
	return nil
}
