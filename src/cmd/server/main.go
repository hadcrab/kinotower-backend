package main

import (
	"log/slog"

	core_logger "github.com/matuha07/kinotower-go/src/internal/core/logger"
	core_server "github.com/matuha07/kinotower-go/src/internal/core/server"
)

func main() {
	logger := core_logger.FromEnv("kinotower")
	slog.SetDefault(logger)
	logger.Info("starting server", "addr", ":8080")

	server := core_server.NewServer(":8080")

	if err := server.ListenAndServe(); err != nil {
		logger.Error("server stopped with error", "error", err)
	}
}
