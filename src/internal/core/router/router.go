package core_router

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	core_logger "github.com/matuha07/kinotower-go/src/internal/core/logger"
)

type Router struct {
	mux *chi.Mux
}

func NewRouter() *Router {
	r := &Router{mux: chi.NewRouter()}
	r.mux.Use(middleware.RequestID)
	r.mux.Use(core_logger.HTTPMiddleware(slog.Default()))
	r.mux.Use(middleware.Recoverer)

	r.mux.Route("/api/v1", func(rl chi.Router) {
		rl.Get("/", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("Welcome to the API!"))
		})
		rl.Mount("/films", r.filmRoutes())
		rl.Mount("/genders", r.genderRoutes())
	})

	return r
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}
