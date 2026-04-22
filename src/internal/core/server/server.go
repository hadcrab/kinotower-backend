package core_server

import (
	"net/http"

	core_router "github.com/matuha07/kinotower-go/src/internal/core/router"
)

type Server struct {
	http.Server
}

func NewServer(addr string) *Server {
	router := core_router.NewRouter()

	return &Server{
		Server: http.Server{
			Addr:    addr,
			Handler: router,
		},
	}
}
