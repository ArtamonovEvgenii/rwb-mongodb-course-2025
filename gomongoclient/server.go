package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"
)

type ServerConfig struct {
	Host string
	Port int
}

type HTTPServer struct {
	server  *http.Server
	address string
	lgr     *slog.Logger
}

func NewHTTPServer(
	lgr *slog.Logger,
	cfg ServerConfig,
	handler http.Handler,
) (*HTTPServer, error) {
	address := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))

	httpServer := &HTTPServer{
		server: &http.Server{
			Addr:    address,
			Handler: handler,
		},
		lgr:     lgr,
		address: address,
	}
	return httpServer, nil
}

func (s *HTTPServer) Run(ctx context.Context) error {
	var err error
	errCh := make(chan error)

	go func() {
		s.lgr.Info(fmt.Sprintf("http server will be started at %s", s.address))
		errCh <- s.server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		err = s.server.Shutdown(shutdownCtx)
		if err != nil {
			err = fmt.Errorf("http server shutdown error: %w", err)
		}
		s.lgr.Info("http server stopped, reason: context canceled")

	case err = <-errCh:
		err = fmt.Errorf("http server: %w", err)
		s.lgr.Warn("http server stopped, reason: error from channel")
	}

	return err
}
