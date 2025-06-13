package sloglogstash

import (
	"context"
	"encoding/json"
	"net"
	"sync"

	"log/slog"

	slogcommon "github.com/samber/slog-common"
)

type Option struct {
	// log level (default: debug)
	Level slog.Leveler

	// connection to logstash
	Conn net.Conn

	// optional: customize json payload builder
	Converter Converter
	// optional: custom marshaler
	Marshaler func(v any) ([]byte, error)
	// optional: fetch attributes from context
	AttrFromContext []func(ctx context.Context) []slog.Attr

	// optional: see slog.HandlerOptions
	AddSource   bool
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}

func (o Option) NewLogstashHandler() slog.Handler {
	if o.Level == nil {
		o.Level = slog.LevelDebug
	}

	if o.Conn == nil {
		panic("missing logstash connections")
	}

	if o.Converter == nil {
		o.Converter = DefaultConverter
	}

	if o.Marshaler == nil {
		o.Marshaler = json.Marshal
	}

	if o.AttrFromContext == nil {
		o.AttrFromContext = []func(ctx context.Context) []slog.Attr{}
	}

	return &LogstashHandler{
		option: o,
		attrs:  []slog.Attr{},
		groups: []string{},
	}
}

var _ slog.Handler = (*LogstashHandler)(nil)

var wg sync.WaitGroup

type LogstashHandler struct {
	option Option
	attrs  []slog.Attr
	groups []string
}

func (h *LogstashHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.option.Level.Level()
}

func (h *LogstashHandler) Handle(ctx context.Context, record slog.Record) error {
	fromContext := slogcommon.ContextExtractor(ctx, h.option.AttrFromContext)
	message := h.option.Converter(h.option.AddSource, h.option.ReplaceAttr, append(h.attrs, fromContext...), h.groups, &record)

	bytes, err := h.option.Marshaler(message)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = h.option.Conn.Write(append(bytes, byte('\n')))
	}()

	return err
}

func (h *LogstashHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &LogstashHandler{
		option: h.option,
		attrs:  slogcommon.AppendAttrsToGroup(h.groups, h.attrs, attrs...),
		groups: h.groups,
	}
}

func (h *LogstashHandler) WithGroup(name string) slog.Handler {
	// https://cs.opensource.google/go/x/exp/+/46b07846:slog/handler.go;l=247
	if name == "" {
		return h
	}

	return &LogstashHandler{
		option: h.option,
		attrs:  h.attrs,
		groups: append(h.groups, name),
	}
}

// Wait for logging goroutines to finish
func WaitForGoroutines() {
	wg.Wait()
}
