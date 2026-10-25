package mapreduce

type KeyValue struct {
	Key   string
	Value string
}

type Result struct {
	Word  string
	Count int
}

type Group struct {
	Key    string
	Values []string
}
