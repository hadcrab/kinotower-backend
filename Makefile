include .env
export $(shell sed 's/=.*//' .env)

postgres-up:
	docker compose -f docker-compose.yml up -d kinotower-postgres

postgres-down:
	docker compose -f docker-compose.yml stop kinotower-postgres

migrate-create:
	docker compose -f docker-compose.yml run --rm kinotower-postgres-migrate create -ext sql -dir /migrations $(name)

migrate-up:
	docker compose -f docker-compose.yml run --rm \
	kinotower-postgres-migrate \
	-path=/migrations \
	-database "postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@kinotower-postgres:5432/$(POSTGRES_DB)?sslmode=disable" up


migrate-down:
	docker compose -f docker-compose.yml run --rm kinotower-postgres-migrate -path=/migrations -database "postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@kinotower-postgres:5432/$(POSTGRES_DB)?sslmode=disable" down

migrate-version:
	docker compose -f docker-compose.yml run --rm kinotower-postgres-migrate -path=/migrations -database "postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@kinotower-postgres:5432/$(POSTGRES_DB)?sslmode=disable" version

kinotower-run:
	go mod tidy && go run src/cmd/server/main.go