package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Aryagorjipour/surface/internal/connector"
	"github.com/Aryagorjipour/surface/internal/shared"
)

// installTracer starts the shared exporter. Setup failure is logged and the
// process still serves. A nil shutdown means nothing was installed.
func installTracer(ctx context.Context, logger *slog.Logger, endpoint string) func(context.Context) error {
	shutdown, err := shared.InitOTEL(ctx, "connector", logger, endpoint)
	if err != nil {
		logger.Warn("otel init failed, continuing without traces", "err", err)
		return nil
	}
	return shutdown
}

func main() {
	logger := shared.NewLogger("connector")
	if err := run(os.Args[1:], os.LookupEnv, logger, nil); err != nil {
		logger.Error("refused startup", "err", err)
		os.Exit(1)
	}
}

func run(args []string, lookup func(string) (string, bool), logger *slog.Logger, listen func(*http.Server) error) error {
	op, err := connector.ParseOperator(args, lookup)
	if errors.Is(err, connector.ErrHelp) {
		if _, err := fmt.Fprint(os.Stdout, connector.ConnectorHelp()); err != nil {
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	if op.Ready {
		return probeReady(op.TLS.Cert)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if shutdown := installTracer(ctx, logger, op.TraceEndpoint); shutdown != nil {
		defer func() {
			if err := shutdown(context.Background()); err != nil {
				logger.Warn("otel shutdown", "err", err)
			}
		}()
	}
	vaultAddr := ""
	if op.SecretStoreSet {
		vaultAddr = op.SecretStoreAddr
	}
	secrets, err := connector.Load(ctx, vaultAddr)
	if err != nil {
		logger.Error("failed to load secrets", "err", err)
		os.Exit(1)
	}

	store, err := connector.Open(ctx, secrets.TursoURL, secrets.TursoToken, secrets.EncryptionKey)
	if err != nil {
		logger.Error("failed to open store", "err", err)
		os.Exit(1)
	}

	cacheAddr := secrets.RedisAddr
	if op.CacheSet {
		cacheAddr = op.CacheAddr
	}
	cache, err := connector.OpenCache(ctx, cacheAddr, secrets.RedisPassword, secrets.EncryptionKey)
	if err != nil {
		return err
	}
	if cache == nil {
		logger.Info("redis disabled")
	} else {
		logger.Info("redis connected")
	}

	h := connector.NewHandlers(store, cache, logger)
	h.SetBearer(secrets.ConnectorToken)
	h.SetCensus(op.Census)
	h.SetLimits(connector.NewLimits(op.Limits))
	h.SetLifetime(op.RequestLifetime)
	window := 3 * op.Heartbeat
	h.SetSilenceWindow(window)
	keys, err := store.Prune(ctx, time.Now(), window)
	if err != nil {
		logger.Error("prune failed", "err", err)
	} else {
		dropPrunedPeerCache(ctx, cache, logger, keys)
	}

	loadCtx, loadCancel := context.WithTimeout(ctx, 2*time.Second)
	_ = h.LoadRequests(loadCtx)
	loadCancel()

	pruneCtx, pruneCancel := context.WithCancel(context.Background())
	waitPrune := h.StartPrune(pruneCtx, op.Maintenance)

	mux := http.NewServeMux()
	h.Register(mux)
	boxCtx, boxCancel := context.WithCancel(context.Background())
	waitBox, err := h.MountMailbox(boxCtx, mux, op.Mailbox, op.Maintenance)
	if err != nil {
		logger.Error("mailbox", "err", err)
		os.Exit(1)
	}
	if strings.TrimSpace(op.Punch) != "" {
		go func() {
			if err := listenPunch(ctx, op.Punch, h); err != nil && ctx.Err() == nil {
				logger.Error("punch listen", "err", err)
			}
		}()
	}

	srv := &http.Server{
		Addr:         op.Listen,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	serve := listen
	if serve == nil {
		serve = op.TLS.Serve
	}
	go func() {
		logger.Info("connector listening", "addr", srv.Addr)
		if err := serve(srv); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	pruneCancel()
	waitPrune()
	boxCancel()
	if waitBox != nil {
		waitBox()
	}

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

func probeReady(certFile string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	url := "http://127.0.0.1:8080/health"
	if strings.TrimSpace(certFile) != "" {
		url = "https://127.0.0.1:8080/health"
		want, err := os.ReadFile(certFile)
		if err != nil {
			return err
		}
		block, _ := pem.Decode(want)
		if block == nil {
			return fmt.Errorf("not ready")
		}
		leaf, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return err
		}
		client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true,
				VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
					if len(raw) == 0 || !bytes.Equal(raw[0], leaf.Raw) {
						return fmt.Errorf("not ready")
					}
					return nil
				},
			},
		}
	}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32))
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK && bytes.HasPrefix(body, []byte("ok")) {
		return nil
	}
	return fmt.Errorf("not ready")
}

// listenPunch reads UDP tickets. Only a 16-byte payload is recorded. The source is not logged.
func listenPunch(ctx context.Context, addr string, h *connector.Handlers) error {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = pc.Close() }()
	go func() {
		<-ctx.Done()
		_ = pc.Close()
	}()
	buf := make([]byte, 64*1024)
	for {
		n, src, err := pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if n != 16 || src == nil {
			continue
		}
		pkt := make([]byte, n)
		copy(pkt, buf[:n])
		h.NotePunch(pkt, src.String())
	}
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
