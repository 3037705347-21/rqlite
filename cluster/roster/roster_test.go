package roster

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestParseRosterOK(t *testing.T) {
	body := `{
  "nodes": [
    {"id": "n1", "addr": "host1:4002", "voter": true},
    {"id": "n2", "addr": "host2:4002", "voter": false}
  ]
}`
	rs, err := Parse(strings.NewReader(body))
	if err != nil {
		t.Fatalf("failed to parse valid roster: %v", err)
	}
	got := rs.Nodes
	want := []Entry{
		{ID: "n1", Addr: "host1:4002", Voter: true},
		{ID: "n2", Addr: "host2:4002", Voter: false},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parsed roster mismatch, got %+v want %+v", got, want)
	}
}

func TestParseRosterRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"invalid JSON":               `{not json`,
		"unknown field":              `{"nodes":[{"id":"n1","addr":"h:1","voter":true,"address":"h:2"}]}`,
		"trailing JSON document":     `{"nodes":[{"id":"n1","addr":"h:1","voter":true}]}{}`,
		"empty nodes":                `{"nodes":[]}`,
		"missing id":                 `{"nodes":[{"addr":"h:1","voter":true}]}`,
		"empty id":                   `{"nodes":[{"id":"","addr":"h:1","voter":true}]}`,
		"whitespace id":              `{"nodes":[{"id":" n1","addr":"h:1","voter":true}]}`,
		"missing addr":               `{"nodes":[{"id":"n1","voter":true}]}`,
		"addr without port":          `{"nodes":[{"id":"n1","addr":"host1","voter":true}]}`,
		"addr with non-numeric port": `{"nodes":[{"id":"n1","addr":"host1:raft","voter":true}]}`,
		"addr with port zero":        `{"nodes":[{"id":"n1","addr":"host1:0","voter":true}]}`,
		"addr with port too large":   `{"nodes":[{"id":"n1","addr":"host1:70000","voter":true}]}`,
		"addr missing host":          `{"nodes":[{"id":"n1","addr":":4002","voter":true}]}`,
		"duplicate ids":              `{"nodes":[{"id":"n1","addr":"h1:1","voter":true},{"id":"n1","addr":"h2:2","voter":true}]}`,
		"duplicate addrs":            `{"nodes":[{"id":"n1","addr":"h:1","voter":true},{"id":"n2","addr":"h:1","voter":false}]}`,
		"multiple problems":          `{"nodes":[{"id":"","addr":"bad"},{"id":"n2","addr":"h2:2","voter":false}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse(strings.NewReader(body)); err == nil {
				t.Fatalf("expected error parsing roster %q", name)
			}
		})
	}
}

func TestParseRosterNilReader(t *testing.T) {
	if _, err := Parse(nil); err == nil {
		t.Fatal("expected error for nil reader")
	}
}

func TestValidateRosterAllowsIPv6(t *testing.T) {
	rs := &Roster{Nodes: []Entry{
		{ID: "n1", Addr: "[::1]:4002", Voter: true},
		{ID: "n2", Addr: "[2001:db8::1]:4002", Voter: false},
	}}
	if err := rs.Validate(); err != nil {
		t.Fatalf("IPv6 addresses should be valid: %v", err)
	}
}

func TestLoadFileRoster(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "roster.json")
	if err := os.WriteFile(path, []byte(`{"nodes":[{"id":"n1","addr":"h:1","voter":true}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	rs, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile failed: %v", err)
	}
	if got, want := rs.Nodes[0].ID, "n1"; got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}
