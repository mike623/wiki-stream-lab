package main

import "testing"

// These cases all fail argument validation before any broker connection, so
// they need no running Kafka.
func TestRunUsageErrors(t *testing.T) {
	cases := [][]string{
		{},                  // no args
		{"topics"},          // missing subcommand
		{"nottopics", "x"},  // wrong group
		{"topics", "bogus"}, // unknown subcommand
	}
	for _, args := range cases {
		if err := run(args); err == nil {
			t.Errorf("run(%q) = nil, want error", args)
		}
	}
}
