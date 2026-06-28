package event

import (
	"errors"
	"testing"
)

func TestPageKey(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
		errIs   error
	}{
		{
			name: "wiki and title",
			raw:  `{"wiki":"enwiki","title":"Example","extra":"ignored"}`,
			want: "enwiki:Example",
		},
		{
			name:    "missing title is ErrInvalid",
			raw:     `{"wiki":"enwiki"}`,
			wantErr: true,
			errIs:   ErrInvalid,
		},
		{
			name:    "missing wiki is ErrInvalid",
			raw:     `{"title":"Example"}`,
			wantErr: true,
			errIs:   ErrInvalid,
		},
		{
			name:    "malformed json is not ErrInvalid",
			raw:     `{"wiki":`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PageKey([]byte(tt.raw))
			if tt.wantErr {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				if tt.errIs != nil && !errors.Is(err, tt.errIs) {
					t.Errorf("err = %v, want errors.Is %v", err, tt.errIs)
				}
				if tt.name == "malformed json is not ErrInvalid" && errors.Is(err, ErrInvalid) {
					t.Errorf("err = %v, must not be ErrInvalid", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("PageKey = %q, want %q", got, tt.want)
			}
		})
	}
}
