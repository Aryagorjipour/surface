package main

import (
	"context"
	"log/slog"
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
	if err := run(os.LookupEnv, logger, func(srv *http.Server) error {
		return srv.ListenAndServe()
	}); err != nil {
		logger.Error("refused startup", "err", err)
		os.Exit(1)
	}
}

func run(lookup func(string) (string, bool), logger *slog.Logger, listen func(*http.Server) error) error {
	cfg, err := connector.ParseLimitConfig(lookup)
	if err != nil {
		return err
	}
	expiry, err := connector.ParseExpiryConfig(lookup)
	if err != nil {
		return err
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

	var cache *connector.RedisCache
	if secrets.RedisAddr != "" && secrets.RedisAddr != "redis:6379" {
		cache = connector.NewRedis(secrets.RedisAddr, secrets.RedisPassword)
		pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
		err := cache.Ping(pingCtx)
		pingCancel()
		if err != nil {
			logger.Warn("redis unavailable, continuing without cache", "err", err)
			cache = nil
		} else {
			logger.Info("redis connected")
		}
	} else {
		logger.Info("redis disabled")
	}

	h := connector.NewHandlers(store, cache, logger)
	h.SetLimits(connector.NewLimits(cfg))
	h.SetLifetime(expiry.RequestLifetime)
	h.SetSilenceWindow(expiry.SilenceWindow)
	keys, err := store.Prune(ctx, time.Now(), expiry.SilenceWindow)
	if err != nil {
		logger.Error("prune failed", "err", err)
	} else {
		dropPrunedPeerCache(ctx, cache, logger, keys)
	}

	pruneCtx, pruneCancel := context.WithCancel(context.Background())
	waitPrune := h.StartPrune(pruneCtx, time.Second)

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
		if err := listen(srv); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	pruneCancel()
	waitPrune()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "err", err)
	}
	if cache != nil {
		if err := cache.Close(); err != nil {
			logger.Error("redis close", "err", err)
		}
	}
	if err := store.Close(); err != nil {
		logger.Error("store close", "err", err)
	}
	logger.Info("connector stopped")
	return nil
}

// dropPrunedPeerCache removes the Redis copy of every peer row the pre-listen prune deleted.
// The log is the operation and the count. A pass that removes nothing logs nothing.
func dropPrunedPeerCache(ctx context.Context, cache *connector.RedisCache, logger *slog.Logger, keys []string) {
	for _, key := range keys {
		_ = cache.DelPeer(ctx, key)
	}
	if len(keys) > 0 && logger != nil {
		logger.Info("pruned peers", "count", len(keys))
	}
}
