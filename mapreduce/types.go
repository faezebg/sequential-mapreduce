package mapreduce

// KeyValue represents an intermediate key-value pair.
type KeyValue struct {
	Key   string
	Value string
}

// Result represents the final word count.
type Result struct {
	Word  string
	Count int
}

type Group struct {
	Key    string
	Values []string
}
