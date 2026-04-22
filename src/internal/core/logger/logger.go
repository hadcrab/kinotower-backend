package core_logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	reset  = "\033[0m"
	bold   = "\033[1m"
	gray   = "\033[90m"
	cyan   = "\033[36m"
	green  = "\033[32m"
	yellow = "\033[33m"
	red    = "\033[31m"
	blue   = "\033[34m"
)

func New(service, level string, json bool) *slog.Logger {
	lvl := parseLevel(level)
	fileHandler := newFileHandler(service, lvl, json)

	var h slog.Handler
	if json {
		console := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
		h = joinHandlers(console, fileHandler)
	} else {
		console := &prettyHandler{w: os.Stdout, lvl: lvl, mu: &sync.Mutex{}, color: true}
		h = joinHandlers(console, fileHandler)
	}

	log := slog.New(h)
	if service != "" {
		log = log.With("service", service)
	}
	return log
}

func FromEnv(service string) *slog.Logger {
	return New(service, os.Getenv("LOG_LEVEL"), strings.EqualFold(os.Getenv("LOG_FORMAT"), "json"))
}

func newFileHandler(service string, lvl slog.Level, json bool) slog.Handler {
	fileWriter, err := newDailyFileWriter(logDirFromEnv(), service)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger: file logging disabled: %v\n", err)
		return nil
	}

	if json {
		return slog.NewJSONHandler(fileWriter, &slog.HandlerOptions{Level: lvl})
	}

	return &prettyHandler{w: fileWriter, lvl: lvl, mu: &sync.Mutex{}, color: false}
}

func joinHandlers(console slog.Handler, extra slog.Handler) slog.Handler {
	if extra == nil {
		return console
	}

	return &teeHandler{handlers: []slog.Handler{console, extra}}
}

func logDirFromEnv() string {
	dir := strings.TrimSpace(os.Getenv("LOG_DIR"))
	if dir == "" {
		return "logs"
	}

	return dir
}

type prettyHandler struct {
	w     io.Writer
	lvl   slog.Level
	attrs []slog.Attr
	mu    *sync.Mutex
	color bool
}

func (h *prettyHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.lvl }
func (h *prettyHandler) WithGroup(name string) slog.Handler           { return h }
func (h *prettyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &prettyHandler{w: h.w, lvl: h.lvl, mu: h.mu, attrs: append(h.attrs, attrs...), color: h.color}
}

func (h *prettyHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder

	b.WriteString(styled(h.color, gray, r.Time.Format("15:04:05")) + "  ")
	b.WriteString(badge(r.Level, h.color) + "  ")
	b.WriteString(styled(h.color, bold, r.Message))

	each(h.attrs, r, func(k string, v any) {
		k = styled(h.color, cyan, k)
		b.WriteString("  " + k + "=" + quote(fmt.Sprint(v)))
	})

	b.WriteByte('\n')
	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, b.String())
	return err
}

func each(base []slog.Attr, r slog.Record, fn func(string, any)) {
	for _, a := range base {
		if a.Key != "" {
			fn(a.Key, a.Value.Resolve().Any())
		}
	}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key != "" {
			fn(a.Key, a.Value.Resolve().Any())
		}
		return true
	})
}

func badge(l slog.Level, color bool) string {
	if !color {
		switch {
		case l >= slog.LevelError:
			return "ERR"
		case l >= slog.LevelWarn:
			return "WRN"
		case l >= slog.LevelInfo:
			return "INF"
		default:
			return "DBG"
		}
	}

	switch {
	case l >= slog.LevelError:
		return red + bold + "ERR" + reset
	case l >= slog.LevelWarn:
		return yellow + bold + "WRN" + reset
	case l >= slog.LevelInfo:
		return green + bold + "INF" + reset
	default:
		return blue + bold + "DBG" + reset
	}
}

func styled(color bool, ansi, text string) string {
	if !color {
		return text
	}

	return ansi + text + reset
}

func quote(s string) string {
	if strings.ContainsAny(s, " \t") {
		return `"` + s + `"`
	}
	return s
}

func parseLevel(v string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type teeHandler struct {
	handlers []slog.Handler
}

func (h *teeHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, one := range h.handlers {
		if one.Enabled(ctx, l) {
			return true
		}
	}

	return false
}

func (h *teeHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, one := range h.handlers {
		if one.Enabled(ctx, r.Level) {
			if err := one.Handle(ctx, r.Clone()); err != nil {
				return err
			}
		}
	}

	return nil
}

func (h *teeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, 0, len(h.handlers))
	for _, one := range h.handlers {
		next = append(next, one.WithAttrs(attrs))
	}

	return &teeHandler{handlers: next}
}

func (h *teeHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, 0, len(h.handlers))
	for _, one := range h.handlers {
		next = append(next, one.WithGroup(name))
	}

	return &teeHandler{handlers: next}
}

type dailyFileWriter struct {
	dir     string
	service string
	date    string
	file    *os.File
	mu      sync.Mutex
}

func newDailyFileWriter(dir, service string) (*dailyFileWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	w := &dailyFileWriter{dir: dir, service: sanitizeService(service)}
	if err := w.rotateIfNeeded(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *dailyFileWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.rotateIfNeeded(); err != nil {
		return 0, err
	}

	return w.file.Write(p)
}

func (w *dailyFileWriter) rotateIfNeeded() error {
	today := time.Now().Format("2006-01-02")
	if w.file != nil && w.date == today {
		return nil
	}

	if w.file != nil {
		_ = w.file.Close()
	}

	path := filepath.Join(w.dir, fmt.Sprintf("%s-%s.log", w.service, today))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	w.file = f
	w.date = today
	return nil
}

func sanitizeService(service string) string {
	name := strings.TrimSpace(service)
	if name == "" {
		return "app"
	}

	name = strings.ReplaceAll(name, " ", "-")
	name = strings.ReplaceAll(name, "/", "-")
	return name
}
