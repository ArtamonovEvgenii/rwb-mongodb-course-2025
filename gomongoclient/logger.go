package main

import (
	"log/slog"
	"os"
)

func NewLogger() *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource:   false,
		Level:       slog.LevelDebug,
		ReplaceAttr: nil,
	})
	lgr := slog.New(handler)

	return lgr
}

func Err(err error) slog.Attr {
	return slog.String("error", err.Error())
}
