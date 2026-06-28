package event

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return b
}

func TestParseRaw(t *testing.T) {
	tests := []struct {
		name     string
		fixture  string
		wantErr  bool
		errIs    error // err must satisfy errors.Is(err, errIs)
		errIsNot error // err must NOT satisfy errors.Is(err, errIsNot)
		check    func(t *testing.T, e RawEvent)
	}{
		{
			name:    "valid categorize has nil revision and length",
			fixture: "categorize.json",
			check: func(t *testing.T, e RawEvent) {
				if e.Type != "categorize" {
					t.Errorf("type = %q, want categorize", e.Type)
				}
				if e.Wiki != "commonswiki" {
					t.Errorf("wiki = %q, want commonswiki", e.Wiki)
				}
				if e.Meta.ID == "" {
					t.Error("meta.id empty, want a uuid")
				}
				if !e.Bot {
					t.Error("bot = false, want true")
				}
				if e.Revision != nil {
					t.Errorf("revision = %+v, want nil", e.Revision)
				}
				if e.Length != nil {
					t.Errorf("length = %+v, want nil", e.Length)
				}
			},
		},
		{
			name:    "valid edit has revision and length",
			fixture: "edit.json",
			check: func(t *testing.T, e RawEvent) {
				if e.Revision == nil {
					t.Fatal("revision nil, want non-nil")
				}
				if e.Revision.New != 1000001 {
					t.Errorf("revision.new = %d, want 1000001", e.Revision.New)
				}
				if e.Length == nil || e.Length.New != 1240 {
					t.Errorf("length = %+v, want new=1240", e.Length)
				}
				if e.Minor != true {
					t.Error("minor = false, want true")
				}
			},
		},
		{
			name:    "missing meta.id is ErrInvalid",
			fixture: "invalid_missing_meta_id.json",
			wantErr: true,
			errIs:   ErrInvalid,
		},
		{
			name:     "malformed json is a decode error, not ErrInvalid",
			fixture:  "malformed.json",
			wantErr:  true,
			errIsNot: ErrInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := ParseRaw(readFixture(t, tt.fixture))
			if tt.wantErr {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				if tt.errIs != nil && !errors.Is(err, tt.errIs) {
					t.Errorf("error = %v, want errors.Is %v", err, tt.errIs)
				}
				if tt.errIsNot != nil && errors.Is(err, tt.errIsNot) {
					t.Errorf("error = %v, must NOT be %v", err, tt.errIsNot)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, e)
			}
		})
	}
}
