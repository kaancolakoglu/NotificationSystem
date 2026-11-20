package api

import (
	"encoding/json"
	"io"
	"strings"

	"github.com/oapi-codegen/runtime"
)

type unknownFieldError struct {
	quotedName string
}

func (e *unknownFieldError) Error() string {
	return "unknown field " + e.quotedName
}

type strictJSON struct {
	runtime.StdJSONCodec
}

func (strictJSON) UnmarshalRead(r io.Reader, v any) error {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(v)
	if err == nil {
		return nil
	}
	if quotedName, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		return &unknownFieldError{quotedName: quotedName}
	}
	return err
}

func (strictJSON) MarshalWrite(w io.Writer, v any) error {
	return json.NewEncoder(w).Encode(v)
}
