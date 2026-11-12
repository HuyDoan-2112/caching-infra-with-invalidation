// Package item holds the item resource: its types, input validation,
// MySQL storage, and HTTP handlers.
package item

import (
	"strings"
	"time"
	"unicode/utf8"
)

// Limits mirror the items table in deploy/mysql/init/01-schema.sql.
const (
	maxNameLen        = 200  // VARCHAR(200)
	maxDescriptionLen = 2000 // VARCHAR(2000)
)

// Item is one row of the items table, as returned by the API.
type Item struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	Description *string   `json:"description"`
	PriceCents  int64     `json:"price_cents"`
	Currency    string    `json:"currency"`
	Stock       *int32    `json:"stock"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Input is the request body for creating or replacing an item. It holds only
// the fields a client may set; id and timestamps come from MySQL.
type Input struct {
	Name        string  `json:"name"`
	Description *string `json:"description"`
	PriceCents  *int64  `json:"price_cents"`
	Currency    string  `json:"currency"`
	Stock       *int32  `json:"stock"`
}

// Validate checks every field and reports all problems at once, keyed by JSON
// field name. It returns nil when the input is valid.
func (i *Input) Validate() map[string]string {
	errs := make(map[string]string)

	switch {
	case strings.TrimSpace(i.Name) == "":
		errs["name"] = "is required"
	case utf8.RuneCountInString(i.Name) > maxNameLen:
		errs["name"] = "must be at most 200 characters"
	}

	if i.Description != nil && utf8.RuneCountInString(*i.Description) > maxDescriptionLen {
		errs["description"] = "must be at most 2000 characters"
	}

	switch {
	case i.PriceCents == nil:
		errs["price_cents"] = "is required"
	case *i.PriceCents < 0:
		errs["price_cents"] = "must be 0 or greater"
	}

	if !isCurrencyCode(i.Currency) {
		errs["currency"] = "must be 3 uppercase letters, like USD"
	}

	if i.Stock != nil && *i.Stock < 0 {
		errs["stock"] = "must be 0 or greater"
	}

	if len(errs) == 0 {
		return nil
	}
	return errs
}

// isCurrencyCode reports whether s has the shape of an ISO 4217 code: exactly
// three ASCII letters A-Z. It does not check that the code actually exists.
func isCurrencyCode(s string) bool {
	if len(s) != 3 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 'A' || s[i] > 'Z' {
			return false
		}
	}
	return true
}
