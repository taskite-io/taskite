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

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

var errVariablesMustBeObject = errors.New("variables must be a JSON object")

func encodeObject(value any) (string, error) {
	if value == nil {
		return "{}", nil
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode variables: %w", err)
	}
	return normalizeObjectJSON(string(encoded))
}

func normalizeObjectJSON(value string) (string, error) {
	if len(bytes.TrimSpace([]byte(value))) == 0 {
		return "{}", nil
	}

	var object map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewBufferString(value))
	if err := decoder.Decode(&object); err != nil {
		return "", fmt.Errorf("%w: %v", errVariablesMustBeObject, err)
	}
	if object == nil {
		return "", errVariablesMustBeObject
	}

	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return "", fmt.Errorf("%w: multiple JSON values", errVariablesMustBeObject)
	} else if !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("%w: %v", errVariablesMustBeObject, err)
	}

	return value, nil
}

type variablesState struct {
	value string
	err   error
}

func defaultVariables() variablesState {
	return variablesState{value: "{}"}
}

func (state *variablesState) fromValue(value any) {
	if state.err != nil {
		return
	}
	state.value, state.err = encodeObject(value)
}

func (state *variablesState) fromString(value string) {
	if state.err != nil {
		return
	}
	state.value, state.err = normalizeObjectJSON(value)
}
