package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Aryagorjipour/surface/internal/connector"
	"github.com/Aryagorjipour/surface/internal/shared"
)

func main() {
	logger := shared.NewLogger("connector")
	cfg, err := connector.ParseLimitConfig(os.LookupEnv)
	if err != nil {
		logger.Error("refused startup", "err", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown, err := connector.InitOTEL(ctx, "connector", logger)
	if err != nil {
		logger.Warn("otel init failed, continuing without traces", "err", err)
	} else {
		defer func() {
			if err := shutdown(context.Background()); err != nil {
				logger.Warn("otel shutdown", "err", err)
			}
		}()
	}
	secrets, err := connector.Load(ctx)
	if err != nil {
		logger.Error("failed to load secrets", "err", err)
		os.Exit(1)
	}

	store, err := connector.Open(ctx, secrets.TursoURL, secrets.TursoToken, secrets.EncryptionKey)
	if err != nil {
		logger.Error("failed to open store", "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("store close", "err", err)
		}
	}()

	var cache *connector.RedisCache
	if secrets.RedisAddr != "" && secrets.RedisAddr != "redis:6379" {
		cache = connector.NewRedis(secrets.RedisAddr, secrets.RedisPassword)
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := cache.Ping(pingCtx)
		cancel()
		if err != nil {
			logger.Warn("redis unavailable, continuing without cache", "err", err)
			cache = nil
		} else {
			logger.Info("redis connected")
			defer func() {
				if err := cache.Close(); err != nil {
					logger.Error("redis close", "err", err)
				}
			}()
		}
	} else {
		logger.Info("redis disabled")
	}

	h := connector.NewHandlers(store, cache, logger)
	h.SetLimits(connector.NewLimits(cfg))
	mux := http.NewServeMux()
	h.Register(mux)

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info("connector listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "err", err)
	}
	logger.Info("connector stopped")
}
