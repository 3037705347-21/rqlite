package proto

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

// Test_IdempotencyKeyRoundTrip verifies the additive idempotencyKey field
// survives marshal/unmarshal alongside pre-existing Request fields.
func Test_IdempotencyKeyRoundTrip(t *testing.T) {
	in := &Request{
		Transaction:    true,
		Statements:     []*Statement{{Sql: "UPDATE t SET n = n - 1"}},
		IdempotencyKey: "order-123",
	}
	b, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("failed to marshal request: %s", err)
	}
	var out Request
	if err := proto.Unmarshal(b, &out); err != nil {
		t.Fatalf("failed to unmarshal request: %s", err)
	}
	if out.GetIdempotencyKey() != "order-123" {
		t.Fatalf("expected idempotency key round-trip, got %q", out.GetIdempotencyKey())
	}
	if !out.GetTransaction() || len(out.GetStatements()) != 1 {
		t.Fatalf("pre-existing fields lost during round-trip: %+v", &out)
	}
}

// Test_DuplicatedRoundTrip verifies the additive duplicated field survives
// marshal/unmarshal of ExecuteQueryResponse.
func Test_DuplicatedRoundTrip(t *testing.T) {
	in := &ExecuteQueryResponse{Duplicated: true}
	b, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("failed to marshal response: %s", err)
	}
	var out ExecuteQueryResponse
	if err := proto.Unmarshal(b, &out); err != nil {
		t.Fatalf("failed to unmarshal response: %s", err)
	}
	if !out.GetDuplicated() {
		t.Fatalf("expected duplicated flag round-trip")
	}
}

// Test_UnknownFieldTolerance verifies that messages carrying fields unknown
// to this build are parsed without error and that the unknown data is
// preserved on re-marshal, as required for rolling upgrades between mixed
// cluster versions.
func Test_UnknownFieldTolerance(t *testing.T) {
	// Field 7 of Request is not declared in this build; append it manually
	// to the wire bytes produced by this build.
	in := &Request{IdempotencyKey: "order-123"}
	b, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("failed to marshal request: %s", err)
	}
	// Append a length-delimited unknown field (tag 7, wire type 2).
	b = append(b, 0x3a) // (7<<3)|2
	b = append(b, 0x03) // length 3
	b = append(b, []byte("foo")...)

	var out Request
	if err := proto.Unmarshal(b, &out); err != nil {
		t.Fatalf("old build must tolerate unknown fields, got: %s", err)
	}
	if out.GetIdempotencyKey() != "order-123" {
		t.Fatalf("known field lost while parsing unknown fields: %q", out.GetIdempotencyKey())
	}
	re, err := proto.Marshal(&out)
	if err != nil {
		t.Fatalf("failed to re-marshal request: %s", err)
	}
	if len(re) < 5 || string(re[len(re)-3:]) != "foo" {
		t.Fatalf("unknown field bytes not preserved on re-marshal: %v", re)
	}
}
