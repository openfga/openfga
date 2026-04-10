package server

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/openfga/openfga/pkg/logger"
)

// RegisterImportHandlers registers HTTP handlers for bulk import endpoints.
// These bypass gRPC interceptors and are intended for testing only.
func RegisterImportHandlers(mux *http.ServeMux, svr *Server, l logger.Logger) {
	mux.HandleFunc("POST /stores/{store_id}/import", handleCreateImport(svr, l))
	mux.HandleFunc("GET /stores/{store_id}/imports/{import_id}", handleGetImportStatus(svr, l))
}

func handleCreateImport(svr *Server, l logger.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		storeID := r.PathValue("store_id")
		if storeID == "" {
			http.Error(w, "store_id is required", http.StatusBadRequest)
			return
		}

		format := r.URL.Query().Get("format")
		if format == "" {
			format = "jsonl"
		}
		modelID := r.URL.Query().Get("authorization_model_id")

		tmpFile, err := os.CreateTemp("", "openfga-import-*")
		if err != nil {
			l.Error("failed to create temp file for import", zap.Error(err))
			http.Error(w, "failed to create temp file", http.StatusInternalServerError)
			return
		}

		if _, err := io.Copy(tmpFile, r.Body); err != nil {
			tmpFile.Close()
			l.Error("failed to write request body to temp file", zap.Error(err))
			http.Error(w, "failed to read request body", http.StatusInternalServerError)
			return
		}
		tmpFile.Close()

		resp, err := svr.CreateImport(r.Context(), &CreateImportRequest{
			StoreID:              storeID,
			AuthorizationModelID: modelID,
			Source:               "file://" + tmpFile.Name(),
			Format:               format,
		})
		if err != nil {
			l.Error("CreateImport failed", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"import_id": resp.ImportID,
			"status":    resp.Status,
		})
	}
}

type importStatusJSON struct {
	ImportID       string     `json:"import_id"`
	Status         string     `json:"status"`
	TuplesImported int64      `json:"tuples_imported"`
	TuplesFailed   int64      `json:"tuples_failed"`
	TuplesTotal    int64      `json:"tuples_total"`
	ErrorMessage   string     `json:"error_message,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
}

func handleGetImportStatus(svr *Server, l logger.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		storeID := r.PathValue("store_id")
		importID := r.PathValue("import_id")

		resp, err := svr.GetImportStatus(r.Context(), &GetImportStatusRequest{
			StoreID:  storeID,
			ImportID: importID,
		})
		if err != nil {
			l.Error("GetImportStatus failed", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(importStatusJSON{
			ImportID:       resp.ImportID,
			Status:         resp.Status,
			TuplesImported: resp.TuplesImported,
			TuplesFailed:   resp.TuplesFailed,
			TuplesTotal:    resp.TuplesTotal,
			ErrorMessage:   resp.ErrorMessage,
			CreatedAt:      resp.CreatedAt,
			CompletedAt:    resp.CompletedAt,
		})
	}
}
