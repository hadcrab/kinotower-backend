package core_server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	core_db "github.com/hadcrab/kinotower-backend/src/internal/db"

	"github.com/go-chi/chi/v5"
)

type Server struct {
	http.Server
	db *core_db.DB
}

func NewServer(addr string) *Server {
	logger := slog.Default()

	dbConn, err := core_db.NewPostgresFromEnv()
	if err != nil {
		logger.Error("failed to connect to db", "error", err)
	}

	var filmRepo core_db.FilmRepository
	var genderRepo core_db.GenderRepository
	var countryRepo core_db.CountryRepository
	var categoryRepo core_db.CategoryRepository
	if dbConn != nil {
		filmRepo = core_db.NewFilmRepository(dbConn)
		genderRepo = core_db.NewGenderRepository(dbConn)
		countryRepo = core_db.NewCountryRepository(dbConn)
		categoryRepo = core_db.NewCategoryRepository(dbConn)
	}

	mux := chi.NewRouter()

	mux.Get("/api/v1", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Welcome to the API!"))
	})

	mux.Route("/api/v1/films", func(r chi.Router) {
		r.Get("/", listFilmsHandler(filmRepo))
		r.Get("/{id}", getFilmHandler(filmRepo))
		r.Get("/{id}/reviews", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "not implemented", http.StatusNotImplemented)
		})
	})

	mux.Get("/api/v1/genders", listGendersHandler(genderRepo))
	mux.Get("/api/v1/countries", listCountriesHandler(countryRepo))
	mux.Get("/api/v1/categories", listCategoriesHandler(categoryRepo))

	s := &Server{
		Server: http.Server{
			Addr:    addr,
			Handler: mux,
		},
		db: dbConn,
	}

	return s
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func parseIntOrDefault(s string, def int) int {
	if s == "" {
		return def
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return i
}

func listFilmsHandler(repo core_db.FilmRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if repo == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"message": "database unavailable"})
			return
		}

		q := r.URL.Query()
		page := parseIntOrDefault(q.Get("page"), 1)
		size := parseIntOrDefault(q.Get("size"), 10)
		sortBy := q.Get("sortBy")
		if sortBy == "" {
			sortBy = "name"
		}
		sortDir := q.Get("sortDir")
		if sortDir == "" {
			sortDir = "asc"
		}
		category := parseIntOrDefault(q.Get("category"), 0)
		country := parseIntOrDefault(q.Get("country"), 0)
		search := q.Get("search")

		result, err := repo.ListFilms(r.Context(), page, size, sortBy, sortDir, category, country, search)
		if err != nil {
			log := slog.Default()
			log.Error("ListFilms failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"message": "internal error"})
			return
		}

		writeJSON(w, http.StatusOK, result)
	}
}

func getFilmHandler(repo core_db.FilmRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if repo == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"message": "database unavailable"})
			return
		}
		idStr := chi.URLParam(r, "id")
		id, err := strconv.Atoi(idStr)
		if err != nil || id <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"message": "invalid id"})
			return
		}

		film, err := repo.GetFilmByID(r.Context(), id)
		if err != nil {
			if err.Error() == "not found" {
				writeJSON(w, http.StatusNotFound, map[string]string{"message": "Film not found"})
				return
			}
			slog.Default().Error("GetFilmByID failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"message": "internal error"})
			return
		}

		writeJSON(w, http.StatusOK, film)
	}
}

func listGendersHandler(repo core_db.GenderRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if repo == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"message": "database unavailable"})
			return
		}
		genders, err := repo.ListGenders(r.Context())
		if err != nil {
			slog.Default().Error("ListGenders failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"message": "internal error"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"genders": genders})
	}
}

func listCountriesHandler(repo core_db.CountryRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if repo == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"message": "database unavailable"})
			return
		}
		countries, err := repo.ListCountries(r.Context())
		if err != nil {
			slog.Default().Error("ListCountries failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"message": "internal error"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"countries": countries})
	}
}

func listCategoriesHandler(repo core_db.CategoryRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if repo == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"message": "database unavailable"})
			return
		}
		categories, err := repo.ListCategories(r.Context())
		if err != nil {
			slog.Default().Error("ListCategories failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"message": "internal error"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"categories": categories})
	}
}
