package item_test

import (
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/item"
)

// ptr returns a pointer to v, for the Input fields that are pointers.
func ptr[T any](v T) *T { return &v }

// validInput returns an Input that passes Validate. Each test case changes
// one field, so a failure points at exactly one rule.
func validInput() item.Input {
	return item.Input{
		Name:        "Keyboard",
		Description: ptr("Mechanical, 87 keys"),
		PriceCents:  ptr(int64(12999)),
		Currency:    "USD",
		Stock:       ptr(int32(10)),
	}
}

func TestInputValidate(t *testing.T) {
	tests := []struct {
		name      string
		modify    func(in *item.Input)
		wantField string // "" means the input is valid
	}{
		{"valid", func(*item.Input) {}, ""},
		{"description and stock omitted", func(in *item.Input) { in.Description, in.Stock = nil, nil }, ""},
		{"empty description", func(in *item.Input) { in.Description = ptr("") }, ""},

		{"name empty", func(in *item.Input) { in.Name = "" }, "name"},
		{"name only spaces", func(in *item.Input) { in.Name = "   " }, "name"},
		{"name 200 chars", func(in *item.Input) { in.Name = strings.Repeat("a", 200) }, ""},
		{"name 201 chars", func(in *item.Input) { in.Name = strings.Repeat("a", 201) }, "name"},
		// 200 characters but 600 bytes: the limit counts characters, like VARCHAR.
		{"name 200 multibyte chars", func(in *item.Input) { in.Name = strings.Repeat("ạ", 200) }, ""},

		{"description 2000 chars", func(in *item.Input) { in.Description = ptr(strings.Repeat("ạ", 2000)) }, ""},
		{"description 2001 chars", func(in *item.Input) { in.Description = ptr(strings.Repeat("a", 2001)) }, "description"},

		{"price missing", func(in *item.Input) { in.PriceCents = nil }, "price_cents"},
		{"price zero", func(in *item.Input) { in.PriceCents = ptr(int64(0)) }, ""},
		{"price negative", func(in *item.Input) { in.PriceCents = ptr(int64(-1)) }, "price_cents"},

		{"currency missing", func(in *item.Input) { in.Currency = "" }, "currency"},
		{"currency lowercase", func(in *item.Input) { in.Currency = "usd" }, "currency"},
		{"currency too short", func(in *item.Input) { in.Currency = "US" }, "currency"},
		{"currency too long", func(in *item.Input) { in.Currency = "USDX" }, "currency"},
		{"currency symbol", func(in *item.Input) { in.Currency = "U$D" }, "currency"},
		{"currency non-ASCII letter", func(in *item.Input) { in.Currency = "ÜSD" }, "currency"},

		{"stock zero", func(in *item.Input) { in.Stock = ptr(int32(0)) }, ""},
		{"stock negative", func(in *item.Input) { in.Stock = ptr(int32(-1)) }, "stock"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := validInput()
			tt.modify(&in)
			errs := in.Validate()

			if tt.wantField == "" {
				if errs != nil {
					t.Fatalf("Validate() = %v, want nil", errs)
				}
				return
			}
			if _, ok := errs[tt.wantField]; !ok || len(errs) != 1 {
				t.Fatalf("Validate() = %v, want exactly one error, on %q", errs, tt.wantField)
			}
		})
	}
}

func TestInputValidateReportsEveryField(t *testing.T) {
	in := item.Input{Name: " ", Currency: "usd", Stock: ptr(int32(-1))} // price missing too
	got := slices.Sorted(maps.Keys(in.Validate()))
	want := []string{"currency", "name", "price_cents", "stock"}
	if !slices.Equal(got, want) {
		t.Fatalf("fields with errors = %v, want %v", got, want)
	}
}
