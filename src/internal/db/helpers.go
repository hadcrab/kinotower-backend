package db

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
)

func PtrString(s string) *string {
	return &s
}

func PtrInt(i int) *int {
	return &i
}

func PtrTime(t time.Time) *time.Time {
	return &t
}

func FormatTimeISO(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	u := t.UTC()
	ms := u.Nanosecond() / 1e6
	return fmt.Sprintf("%s.%03dZ", u.Format("2006-01-02T15:04:05"), ms)
}

func NullStringFromPtr(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: *s, Valid: true}
}

func NullTimeFromPtr(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{Valid: false}
	}
	return sql.NullTime{Time: *t, Valid: true}
}

func NullInt64FromInt(i int) sql.NullInt64 {
	return sql.NullInt64{Int64: int64(i), Valid: true}
}
