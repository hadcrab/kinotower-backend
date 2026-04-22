package core_logger

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

func HTTPMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	return middleware.RequestLogger(&chiFormatter{log})
}

type chiFormatter struct{ log *slog.Logger }

func (f *chiFormatter) NewLogEntry(r *http.Request) middleware.LogEntry {
	log := f.log.With("id", trimReqID(middleware.GetReqID(r.Context())))
	log.Debug("incoming", "method", r.Method, "path", r.URL.Path)
	return &chiEntry{log, r.Method, r.URL.Path}
}

type chiEntry struct {
	log    *slog.Logger
	method string
	path   string
}

func (e *chiEntry) Write(status, bytes int, _ http.Header, elapsed time.Duration, _ any) {
	level := slog.LevelInfo
	if status >= 500 {
		level = slog.LevelError
	} else if status >= 400 {
		level = slog.LevelWarn
	}
	e.log.Log(nil, level, fmt.Sprintf("%s %s", e.method, e.path),
		"status", status,
		"bytes", bytes,
		"duration", elapsed.Round(time.Microsecond),
	)
}

func (e *chiEntry) Panic(v any, stack []byte) {
	e.log.Error("panic", "err", v, "stack", string(stack))
}

func trimReqID(id string) string {
	if i := strings.LastIndex(id, "/"); i >= 0 {
		id = id[i+1:]
	}
	if i := strings.Index(id, "-"); i >= 0 {
		id = id[:i]
	}
	return id
}
