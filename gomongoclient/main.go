package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	err := run(ctx)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	lgr := NewLogger()
	lgr.Info("start service")

	mongoURI := os.Getenv("MONGODB_URI")
	if mongoURI == "" {
		return fmt.Errorf("MONGODB_URI environment variable not set")
	}

	mongoDBName := os.Getenv("MONGODB_NAME")
	if mongoDBName == "" {
		return fmt.Errorf("MONGODB_NAME environment variable not set")
	}

	stocksCollectionName := os.Getenv("MONGODB_STOCKS_COLLECTION_NAME")
	if stocksCollectionName == "" {
		return fmt.Errorf("MONGODB_STOCKS_COLLECTION_NAME environment variable not set")
	}

	mongo, err := NewMongo(ctx, mongoURI, mongoDBName, stocksCollectionName, lgr)
	if err != nil {
		return fmt.Errorf("create mongo: %w", err)
	}
	defer mongo.Close()

	lgr.Info("checkpoint")

	handler := CreateHandler(mongo)

	cfg := ServerConfig{
		Host: "0.0.0.0",
		Port: 8888,
	}
	httpServer, err := NewHTTPServer(lgr, cfg, handler)
	if err != nil {
		return fmt.Errorf("create http server: %w", err)
	}

	g, egCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return httpServer.Run(egCtx)
	})
	return g.Wait()
}

type stocksRepository interface {
	FindHighStock(ctx context.Context, high float64) (stockSymbol string, err error)
}

func CreateHandler(sr stocksRepository) http.Handler {
	highStockHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req highStockRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Close the request body to prevent resource leaks.
		defer func() {
			_ = r.Body.Close()
		}()

		stockSymbol, err := sr.FindHighStock(r.Context(), req.High)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if stockSymbol == "" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprintf(w, "stockSymbol: not found")
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "stockSymbol: %s", stockSymbol)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/high", highStockHandler)
	return mux
}

type highStockRequest struct {
	High float64 `json:"high"`
}
