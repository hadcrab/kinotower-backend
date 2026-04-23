package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	authpkg "github.com/hadcrab/kinotower-backend/src/internal/auth"
	"github.com/go-chi/chi/v5"
)

type ctxKey int

const userIDKey ctxKey = 1

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func RegisterAuthRoutes(r chi.Router, svc *authpkg.Service) {
	r.Post("/auth/signup", signupHandler(svc))
	r.Post("/auth/signin", signinHandler(svc))
	r.Post("/auth/signout", signoutHandler(svc))
}

type signupPayload struct {
	FIO      string `json:"fio"`
	Email    string `json:"email"`
	Password string `json:"password"`
	Birthday string `json:"birthday,omitempty"`
	GenderID int    `json:"gender_id"`
}

func signupHandler(svc *authpkg.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var p signupPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"message": "invalid payload"})
			return
		}
		var b *time.Time
		if strings.TrimSpace(p.Birthday) != "" {
			t, err := time.Parse("2006-01-02", p.Birthday)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"message": "invalid birthday"})
				return
			}
			b = &t
		}
		req := authpkg.SignupRequest{
			FIO:      p.FIO,
			Email:    p.Email,
			Password: p.Password,
			Birthday: b,
			GenderID: p.GenderID,
		}
		token, id, fio, err := svc.Signup(r.Context(), req)
		if err != nil {
			if err.Error() == "email exists" {
				writeJSON(w, http.StatusBadRequest, map[string]string{"message": "email exists"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"message": "internal error"})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"status": "success", "token": token, "id": id, "fio": fio})
	}
}

type signinPayload struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func signinHandler(svc *authpkg.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var p signinPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"message": "invalid payload"})
			return
		}
		req := authpkg.SigninRequest{Email: p.Email, Password: p.Password}
		token, id, fio, err := svc.Signin(r.Context(), req)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"status": "invalid", "message": "Wrong email or password"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "success", "token": token, "id": id, "fio": fio})
	}
}

func signoutHandler(svc *authpkg.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		if h == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"message": "missing authorization"})
			return
		}
		parts := strings.Fields(h)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"message": "invalid authorization header"})
			return
		}
		token := parts[1]
		svc.Signout(token)
		writeJSON(w, http.StatusOK, map[string]string{"status": "success"})
	}
}

func AuthMiddleware(svc *authpkg.Service) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := r.Header.Get("Authorization")
			if h == "" {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"message": "unauthorized"})
				return
			}
			parts := strings.Fields(h)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"message": "unauthorized"})
				return
			}
			token := parts[1]
			uid, err := svc.ValidateToken(token)
			if err != nil {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"message": "unauthorized"})
				return
			}
			ctx := context.WithValue(r.Context(), userIDKey, uid)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func GetUserID(r *http.Request) (int, bool) {
	v := r.Context().Value(userIDKey)
	if v == nil {
		return 0, false
	}
	switch t := v.(type) {
	case int:
		return t, true
	case string:
		i, err := strconv.Atoi(t)
		if err != nil {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}
