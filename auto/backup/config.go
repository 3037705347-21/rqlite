package backup

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/rqlite/rqlite/v10/auto"
	"github.com/rqlite/rqlite/v10/auto/aws"
	"github.com/rqlite/rqlite/v10/auto/file"
	"github.com/rqlite/rqlite/v10/auto/gcp"
)

// Config is the config file format for the upload service. Version 1 configures
// a single storage target via Type/Sub/Timestamp. Version 2 configures a list
// of Destinations, with every backup generation pushed to all of them.
type Config struct {
	Version      int                 `json:"version"`
	Type         auto.StorageType    `json:"type,omitempty"`
	NoCompress   bool                `json:"no_compress,omitempty"`
	Timestamp    bool                `json:"timestamp,omitempty"`
	Vacuum       bool                `json:"vacuum,omitempty"`
	Interval     auto.Duration       `json:"interval"`
	Sub          json.RawMessage     `json:"sub,omitempty"`
	Destinations []DestinationConfig `json:"destinations,omitempty"`
}

// DestinationConfig is one entry in a version-2 multi-destination manifest.
type DestinationConfig struct {
	// Name is an optional human-readable label used in logs and status. If
	// empty, the storage client's String representation is used.
	Name      string           `json:"name,omitempty"`
	Type      auto.StorageType `json:"type"`
	Timestamp bool             `json:"timestamp,omitempty"`
	Sub       json.RawMessage  `json:"sub"`
}

// Destination is a named backup target.
type Destination struct {
	Name   string
	Client StorageClient
}

// NewStorageClient unmarshals the config data and returns the Config and
// StorageClient. It only supports the version-1 single-target config format;
// use NewStorageClients for version-2 multi-destination configs.
func NewStorageClient(data []byte) (*Config, StorageClient, error) {
	cfg, destinations, err := NewStorageClients(data)
	if err != nil {
		return nil, nil, err
	}
	if len(cfg.Destinations) > 0 {
		return nil, nil, errors.New("multi-destination config requires NewStorageClients")
	}
	return cfg, destinations[0].Client, nil
}

// NewStorageClients unmarshals the config data and returns the Config and the
// configured backup Destinations. A version-1 config yields exactly one
// destination; a version-2 config yields one destination per manifest entry.
// Any malformed manifest entry, including failure to construct its storage
// client (e.g. an unwritable file directory), results in an error identifying
// the offending destination.
func NewStorageClients(data []byte) (*Config, []Destination, error) {
	cfg := &Config{}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(cfg); err != nil {
		return nil, nil, err
	}

	if cfg.Version > auto.Version {
		return nil, nil, auto.ErrInvalidVersion
	}
	if cfg.Interval <= 0 {
		return nil, nil, auto.ErrInvalidInterval
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, nil, err
	}

	if len(cfg.Destinations) == 0 {
		if cfg.Type == "" {
			return nil, nil, errors.New(`backup config must set either "type" or "destinations"`)
		}
		if _, ok := raw["destinations"]; ok {
			return nil, nil, errors.New(`"destinations" requires config version 2`)
		}
		sc, err := newStorageClient(cfg.Type, cfg.Timestamp, cfg.Sub)
		if err != nil {
			return nil, nil, err
		}
		return cfg, []Destination{{Name: sc.String(), Client: sc}}, nil
	}

	// Version-2 multi-destination manifest.
	if cfg.Version < 2 {
		return nil, nil, errors.New(`"destinations" requires config version 2`)
	}
	for _, key := range []string{"type", "sub", "timestamp"} {
		if _, ok := raw[key]; ok {
			return nil, nil, fmt.Errorf(`%q is not valid in a version-2 config; configure it per destination`, key)
		}
	}

	destinations := make([]Destination, 0, len(cfg.Destinations))
	usedNames := make(map[string]struct{})
	for i, dCfg := range cfg.Destinations {
		label := dCfg.Name
		if label == "" {
			label = string(dCfg.Type)
		}
		if dCfg.Type == "" {
			return nil, nil, fmt.Errorf("backup destination %q (#%d): missing type", label, i+1)
		}
		if len(bytes.TrimSpace(dCfg.Sub)) == 0 {
			return nil, nil, fmt.Errorf("backup destination %q (#%d): missing sub-config", label, i+1)
		}
		sc, err := newStorageClient(dCfg.Type, dCfg.Timestamp, dCfg.Sub)
		if err != nil {
			return nil, nil, fmt.Errorf("backup destination %q (#%d): %w", label, i+1, err)
		}

		name := dCfg.Name
		if name == "" {
			name = sc.String()
		}
		if _, dup := usedNames[name]; dup {
			if dCfg.Name != "" {
				return nil, nil, fmt.Errorf("duplicate backup destination name %q (#%d)", name, i+1)
			}
			base := name
			for n := 2; ; n++ {
				candidate := fmt.Sprintf("%s #%d", base, n)
				if _, ok := usedNames[candidate]; !ok {
					name = candidate
					break
				}
			}
		}
		usedNames[name] = struct{}{}
		destinations = append(destinations, Destination{Name: name, Client: sc})
	}
	return cfg, destinations, nil
}

// newStorageClient builds a StorageClient for the given type and sub-config.
func newStorageClient(typ auto.StorageType, timestamp bool, sub json.RawMessage) (StorageClient, error) {
	switch typ {
	case auto.StorageTypeS3:
		s3cfg := &aws.S3Config{}
		if err := json.Unmarshal(sub, s3cfg); err != nil {
			return nil, err
		}
		opts := &aws.S3ClientOpts{
			ForcePathStyle: s3cfg.ForcePathStyle,
			Timestamp:      timestamp,
		}
		return aws.NewS3Client(s3cfg.Endpoint, s3cfg.Region, s3cfg.AccessKeyID, s3cfg.SecretAccessKey,
			s3cfg.Bucket, s3cfg.Path, opts)
	case auto.StorageTypeGCS:
		gcsCfg := &gcp.GCSConfig{}
		if err := json.Unmarshal(sub, gcsCfg); err != nil {
			return nil, err
		}
		opts := &gcp.GCSClientOpts{
			Timestamp: timestamp,
		}
		return gcp.NewGCSClient(gcsCfg, opts)
	case auto.StorageTypeFile:
		fileCfg := &file.Config{}
		if err := json.Unmarshal(sub, fileCfg); err != nil {
			return nil, err
		}
		opts := &file.Options{
			Timestamp: timestamp,
		}
		return file.NewClient(fileCfg.Dir, fileCfg.Name, opts)
	default:
		return nil, auto.ErrUnsupportedStorageType
	}
}

// ReadConfigFile reads the config file and returns the data. It also expands
// any environment variables in the config file.
func ReadConfigFile(filename string) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data = []byte(os.ExpandEnv(string(data)))
	return data, nil
}
