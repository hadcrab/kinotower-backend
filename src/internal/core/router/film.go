package core_router

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (r *Router) filmRoutes() http.Handler {
	router := chi.NewRouter()

	router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Films"))
	})
	return router
}
