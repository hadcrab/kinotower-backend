package auth

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	dbpkg "github.com/hadcrab/kinotower-backend/src/internal/db"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

type Service struct {
	repo      dbpkg.UserRepository
	secret    []byte
	ttl       time.Duration
	blacklist map[string]time.Time
	mu        sync.Mutex
}

type SignupRequest struct {
	FIO      string
	Email    string
	Password string
	Birthday *time.Time
	GenderID int
}

type SigninRequest struct {
	Email    string
	Password string
}

func NewService(repo dbpkg.UserRepository, secret string, ttl time.Duration) *Service {
	return &Service{
		repo:      repo,
		secret:    []byte(secret),
		ttl:       ttl,
		blacklist: make(map[string]time.Time),
	}
}

func (s *Service) Signup(ctx context.Context, req SignupRequest) (string, int, string, error) {
	if len(req.FIO) < 2 || len(req.Password) < 6 || len(req.Email) < 4 {
		return "", 0, "", errors.New("validation")
	}
	_, err := s.repo.GetUserByEmail(ctx, req.Email)
	if err == nil {
		return "", 0, "", errors.New("email exists")
	}
	if err != nil && err != dbpkg.ErrNotFound {
		return "", 0, "", err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return "", 0, "", err
	}
	u := &dbpkg.User{
		FIO:       req.FIO,
		Birthday:  req.Birthday,
		GenderID:  req.GenderID,
		Email:     req.Email,
		Password:  string(hash),
		CreatedAt: time.Now().UTC(),
	}
	id, err := s.repo.CreateUser(ctx, u)
	if err != nil {
		return "", 0, "", err
	}
	token, err := s.makeToken(id, req.FIO)
	if err != nil {
		return "", 0, "", err
	}
	return token, id, req.FIO, nil
}

func (s *Service) Signin(ctx context.Context, req SigninRequest) (string, int, string, error) {
	u, err := s.repo.GetUserByEmail(ctx, req.Email)
	if err != nil {
		if err == dbpkg.ErrNotFound {
			return "", 0, "", errors.New("wrong email or password")
		}
		return "", 0, "", err
	}
	if bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(req.Password)) != nil {
		return "", 0, "", errors.New("wrong email or password")
	}
	token, err := s.makeToken(u.ID, u.FIO)
	if err != nil {
		return "", 0, "", err
	}
	return token, u.ID, u.FIO, nil
}

func (s *Service) Signout(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blacklist[token] = time.Now().Add(s.ttl)
}

func (s *Service) ValidateToken(tokenStr string) (int, error) {
	s.mu.Lock()
	exp, ok := s.blacklist[tokenStr]
	s.mu.Unlock()
	if ok {
		if time.Now().Before(exp) {
			return 0, errors.New("token invalidated")
		}
		s.mu.Lock()
		delete(s.blacklist, tokenStr)
		s.mu.Unlock()
	}
	token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return s.secret, nil
	})
	if err != nil {
		return 0, err
	}
	if !token.Valid {
		return 0, errors.New("invalid token")
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return 0, errors.New("invalid claims")
	}
	sub, ok := claims["sub"].(string)
	if !ok {
		return 0, errors.New("invalid subject")
	}
	id, err := strconv.Atoi(sub)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Service) makeToken(id int, fio string) (string, error) {
	now := time.Now()
	exp := now.Add(s.ttl)
	claims := jwt.MapClaims{
		"sub": strconv.Itoa(id),
		"fio": fio,
		"iat": now.Unix(),
		"exp": exp.Unix(),
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return t.SignedString(s.secret)
}
