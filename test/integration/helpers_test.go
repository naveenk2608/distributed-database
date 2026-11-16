package integration

import "log/slog"

func slogDiscard() *slog.Logger { return slog.New(slog.DiscardHandler) }
