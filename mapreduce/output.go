package mapreduce

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

// WriteOutput saves results sorted alphabetically
// in the format "word: count".
func WriteOutput(filename string, results []Result) error {
	// Copy results to avoid modifying the original slice.
	ordered := append([]Result(nil), results...)

	// Sort results alphabetically by word.
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Word < ordered[j].Word
	})

	var builder strings.Builder

	// Build the output content.
	for _, result := range ordered {
		fmt.Fprintf(
			&builder,
			"%s: %d\n",
			result.Word,
			result.Count,
		)
	}

	// Write all results to the output file.
	err := os.WriteFile(
		filename,
		[]byte(builder.String()),
		0644,
	)

	if err != nil {
		return fmt.Errorf(
			"cannot write output file %q: %w",
			filename,
			err,
		)
	}

	return nil
}
