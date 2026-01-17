package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/manjeet13/logbase/internal/config"
	"github.com/manjeet13/logbase/internal/storage"
)

func main() {
	cfg := config.Load()

	engine, err := storage.NewEngineWithConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/kv/", kvHandler(engine))
	mux.HandleFunc("/range", rangeHandler(engine))
	mux.HandleFunc("/batch", batchHandler(engine))

	server := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}
	log.Println("Logbase listening on :" + cfg.HTTPPort)
	log.Fatal(server.ListenAndServe())
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func kvHandler(engine *storage.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/kv/"):]
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodGet:
			val, ok := engine.Get([]byte(key))
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Write(val)

		case http.MethodPut:
			value, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := engine.Put([]byte(key), value); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)

		case http.MethodDelete:
			if err := engine.Delete([]byte(key)); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func rangeHandler(engine *storage.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := r.URL.Query().Get("start")
		end := r.URL.Query().Get("end")

		if start == "" || end == "" {
			http.Error(w, "start and end required", http.StatusBadRequest)
			return
		}

		result, err := engine.ReadKeyRange([]byte(start), []byte(end))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		for k, v := range result {
			w.Write([]byte(k))
			w.Write([]byte("="))
			w.Write(v)
			w.Write([]byte("\n"))
		}
	}
}

func batchHandler(engine *storage.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var data map[string]string
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		entries := make(map[string][]byte)
		for k, v := range data {
			entries[k] = []byte(v)
		}

		if err := engine.BatchPut(entries); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}
