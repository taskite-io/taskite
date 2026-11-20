// Copyright (c) 2026 lzgabel and Taskite contributors
//
// Licensed under the Taskite Community License, Version 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
//     https://github.com/taskite-io/taskite/blob/main/LICENSE
//
// The Software is source-available for non-commercial use and limited evaluation
// use. Other commercial use requires a separate written license. The Software is
// provided "AS IS", without warranties or conditions of any kind.

package commands

import "testing"

func TestNormalizeObjectJSON(t *testing.T) {
	t.Parallel()

	valid := map[string]string{
		"empty":      "",
		"blank":      "  \n",
		"object":     `{"orderId":42}`,
		"whitespace": ` { "paid": true } `,
	}
	for name, input := range valid {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := normalizeObjectJSON(input); err != nil {
				t.Fatalf("expected valid object, got %v", err)
			}
		})
	}

	invalid := []string{
		"null",
		"[]",
		`"value"`,
		"42",
		"true",
		`{} {}`,
		"{} invalid",
	}
	for _, input := range invalid {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			if _, err := normalizeObjectJSON(input); err == nil {
				t.Fatalf("expected %q to be rejected", input)
			}
		})
	}
}

func TestEncodeObject(t *testing.T) {
	t.Parallel()

	encoded, err := encodeObject(map[string]any{"orderId": 42})
	if err != nil {
		t.Fatal(err)
	}
	if encoded != `{"orderId":42}` {
		t.Fatalf("unexpected JSON: %s", encoded)
	}

	if _, err := encodeObject([]string{"not", "an", "object"}); err == nil {
		t.Fatal("expected array to be rejected")
	}
}
