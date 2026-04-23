package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

type Gender struct {
	ID   int    `db:"id" json:"id"`
	Name string `db:"name" json:"name"`
}

type Country struct {
	ID   int    `db:"id" json:"id"`
	Name string `db:"name" json:"name"`
}

type Category struct {
	ID             int          `db:"id" json:"id"`
	Name           string       `db:"name" json:"name"`
	ParentCategory *CategoryRef `json:"parentCategory,omitempty"`
	FilmCount      int          `json:"filmCount,omitempty"`
}

type CategoryRef struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type User struct {
	ID        int        `db:"id" json:"id"`
	FIO       string     `db:"fio" json:"fio"`
	Birthday  *time.Time `db:"birthday" json:"birthday,omitempty"`
	GenderID  int        `db:"gender_id" json:"gender_id"`
	Email     string     `db:"email" json:"email"`
	Password  string     `db:"password" json:"-"`
	CreatedAt time.Time  `db:"created_at" json:"created_at"`
	DeletedAt *time.Time `db:"deleted_at" json:"deleted_at,omitempty"`
}

type Film struct {
	ID            int        `db:"id" json:"id"`
	Name          string     `db:"name" json:"name"`
	CountryID     int        `db:"country_id" json:"country_id"`
	Duration      int        `db:"duration" json:"duration"`
	YearOfIssue   int        `db:"year_of_issue" json:"year_of_issue"`
	Age           int        `db:"age" json:"age"`
	LinkImg       *string    `db:"link_img" json:"link_img,omitempty"`
	LinkKinopoisk *string    `db:"link_kinopoisk" json:"link_kinopoisk,omitempty"`
	LinkVideo     string     `db:"link_video" json:"link_video"`
	CreatedAt     time.Time  `db:"created_at" json:"created_at"`
	DeletedAt     *time.Time `db:"deleted_at" json:"deleted_at,omitempty"`
}

type Review struct {
	ID         int        `db:"id" json:"id"`
	FilmID     int        `db:"film_id" json:"film_id"`
	UserID     int        `db:"user_id" json:"user_id"`
	Message    string     `db:"message" json:"message"`
	CreatedAt  time.Time  `db:"created_at" json:"created_at"`
	IsApproved bool       `db:"is_approved" json:"is_approved"`
	DeletedAt  *time.Time `db:"deleted_at" json:"deleted_at,omitempty"`
}

type Rating struct {
	ID        int       `db:"id" json:"id"`
	FilmID    int       `db:"film_id" json:"film_id"`
	UserID    int       `db:"user_id" json:"user_id"`
	Ball      int       `db:"ball" json:"ball"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

type FilmListItem struct {
	ID            int           `json:"id" db:"id"`
	Name          string        `json:"name" db:"name"`
	Duration      int           `json:"duration" db:"duration"`
	YearOfIssue   int           `json:"year_of_issue" db:"year_of_issue"`
	Age           int           `json:"age" db:"age"`
	LinkImg       *string       `json:"link_img" db:"link_img"`
	LinkKinopoisk *string       `json:"link_kinopoisk" db:"link_kinopoisk"`
	LinkVideo     string        `json:"link_video" db:"link_video"`
	CreatedAt     time.Time     `json:"created_at" db:"created_at"`
	Country       Country       `json:"country"`
	Categories    []CategoryRef `json:"categories"`
	RatingAvg     float64       `json:"ratingAvg"`
	ReviewCount   int           `json:"reviewCount"`
}

type FilmsPage struct {
	Page  int            `json:"page"`
	Size  int            `json:"size"`
	Total int            `json:"total"`
	Films []FilmListItem `json:"films"`
}

type DB struct {
	sqlx *sqlx.DB
}

func NewPostgresFromEnv() (*DB, error) {
	if url := strings.TrimSpace(os.Getenv("POSTGRES_URL")); url != "" {
		return NewPostgres(url)
	}

	user := os.Getenv("POSTGRES_USER")
	pass := os.Getenv("POSTGRES_PASSWORD")
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}
	dbname := os.Getenv("POSTGRES_DB")
	if dbname == "" {
		dbname = "postgres"
	}

	sslmode := os.Getenv("POSTGRES_SSLMODE")
	if sslmode == "" {
		sslmode = "disable"
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, port, dbname, sslmode)
	return NewPostgres(dsn)
}

func NewPostgres(dsn string) (*DB, error) {
	conn, err := sqlx.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(20)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return &DB{sqlx: conn}, nil
}

func (db *DB) Close() error {
	if db.sqlx == nil {
		return nil
	}
	return db.sqlx.Close()
}

type FilmRepository interface {
	ListFilms(ctx context.Context, page, size int, sortBy, sortDir string, categoryID, countryID int, search string) (FilmsPage, error)
	GetFilmByID(ctx context.Context, id int) (*FilmListItem, error)
}

type GenderRepository interface {
	ListGenders(ctx context.Context) ([]Gender, error)
}

type filmRepoSQL struct {
	db *sqlx.DB
}

func NewFilmRepository(db *DB) FilmRepository {
	return &filmRepoSQL{db: db.sqlx}
}

type genderRepoSQL struct {
	db *sqlx.DB
}

func NewGenderRepository(db *DB) GenderRepository {
	return &genderRepoSQL{db: db.sqlx}
}

func sanitizeSortBy(s string) string {
	switch s {
	case "name", "year", "rating":
		return s
	default:
		return "name"
	}
}

func sanitizeSortDir(d string) string {
	if strings.ToLower(d) == "desc" {
		return "desc"
	}
	return "asc"
}

func (r *filmRepoSQL) ListFilms(ctx context.Context, page, size int, sortBy, sortDir string, categoryID, countryID int, search string) (FilmsPage, error) {
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 10
	}
	sortBy = sanitizeSortBy(sortBy)
	sortDir = sanitizeSortDir(sortDir)

	args := []interface{}{}
	where := []string{"f.deleted_at IS NULL"}

	if countryID > 0 {
		args = append(args, countryID)
		where = append(where, fmt.Sprintf("f.country_id = $%d", len(args)))
	}
	if categoryID > 0 {
		args = append(args, categoryID)
		where = append(where, fmt.Sprintf("EXISTS (SELECT 1 FROM categories_films cf WHERE cf.film_id = f.id AND cf.category_id = $%d)", len(args)))
	}
	if search != "" {
		args = append(args, "%"+search+"%")
		where = append(where, fmt.Sprintf("f.name ILIKE $%d", len(args)))
	}

	whereSQL := strings.Join(where, " AND ")

	countQuery := fmt.Sprintf("SELECT COUNT(1) FROM films f WHERE %s", whereSQL)
	var total int
	if err := r.db.GetContext(ctx, &total, countQuery, args...); err != nil {
		return FilmsPage{}, err
	}

	var orderBy string
	switch sortBy {
	case "name":
		orderBy = "f.name"
	case "year":
		orderBy = "f.year_of_issue"
	case "rating":
		orderBy = "COALESCE(r.avg, 0)"
	default:
		orderBy = "f.name"
	}

	limit := size
	offset := (page - 1) * size

	ratingJoin := ""
	if sortBy == "rating" {
		ratingJoin = `LEFT JOIN (
			SELECT film_id, AVG(ball)::numeric(10,2) AS avg
			FROM ratings
			GROUP BY film_id
		) r ON r.film_id = f.id`
	}

	filmQuery := fmt.Sprintf(`
		SELECT f.id, f.name, f.duration, f.year_of_issue, f.age, f.link_img, f.link_kinopoisk, f.link_video, f.created_at,
		       c.id "country.id", c.name "country.name"
		FROM films f
		LEFT JOIN countries c ON c.id = f.country_id
		%s
		WHERE %s
		ORDER BY %s %s
		LIMIT %d OFFSET %d
	`, ratingJoin, whereSQL, orderBy, sortDir, limit, offset)

	rows, err := r.db.QueryxContext(ctx, filmQuery, args...)
	if err != nil {
		return FilmsPage{}, err
	}
	defer rows.Close()

	films := []FilmListItem{}
	filmIDs := []int{}

	for rows.Next() {
		var item FilmListItem
		var countryIDNull sql.NullInt64
		var countryName sql.NullString
		var linkImg sql.NullString
		var linkKinopoisk sql.NullString

		err := rows.Scan(
			&item.ID,
			&item.Name,
			&item.Duration,
			&item.YearOfIssue,
			&item.Age,
			&linkImg,
			&linkKinopoisk,
			&item.LinkVideo,
			&item.CreatedAt,
			&countryIDNull,
			&countryName,
		)
		if err != nil {
			return FilmsPage{}, err
		}
		if linkImg.Valid {
			item.LinkImg = &linkImg.String
		}
		if linkKinopoisk.Valid {
			item.LinkKinopoisk = &linkKinopoisk.String
		}
		if countryIDNull.Valid {
			item.Country = Country{
				ID:   int(countryIDNull.Int64),
				Name: countryName.String,
			}
		}
		films = append(films, item)
		filmIDs = append(filmIDs, item.ID)
	}

	if err := rows.Err(); err != nil {
		return FilmsPage{}, err
	}

	if len(filmIDs) == 0 {
		return FilmsPage{
			Page:  page,
			Size:  len(films),
			Total: total,
			Films: films,
		}, nil
	}

	queryCategories := `
		SELECT cf.film_id, cat.id, cat.name
		FROM categories_films cf
		JOIN categories cat ON cat.id = cf.category_id
		WHERE cf.film_id = ANY($1)
	`
	type catRow struct {
		FilmID  int    `db:"film_id"`
		CatID   int    `db:"id"`
		CatName string `db:"name"`
	}
	catRows := []catRow{}
	if err := r.db.SelectContext(ctx, &catRows, queryCategories, pqArray(filmIDs)); err != nil {
		catRows = []catRow{}
	}

	catMap := map[int][]CategoryRef{}
	for _, cr := range catRows {
		catMap[cr.FilmID] = append(catMap[cr.FilmID], CategoryRef{ID: cr.CatID, Name: cr.CatName})
	}

	queryRatings := `
		SELECT film_id, COALESCE(AVG(ball)::numeric(10,2),0) AS avg, COUNT(1) AS cnt
		FROM ratings
		WHERE film_id = ANY($1)
		GROUP BY film_id
	`
	type ratingRow struct {
		FilmID int             `db:"film_id"`
		Avg    sql.NullFloat64 `db:"avg"`
		Cnt    int             `db:"cnt"`
	}
	ratingRows := []ratingRow{}
	if err := r.db.SelectContext(ctx, &ratingRows, queryRatings, pqArray(filmIDs)); err != nil {
		ratingRows = []ratingRow{}
	}
	ratingMap := map[int]ratingRow{}
	for _, rr := range ratingRows {
		ratingMap[rr.FilmID] = rr
	}

	for i := range films {
		id := films[i].ID
		if cats, ok := catMap[id]; ok {
			films[i].Categories = cats
		} else {
			films[i].Categories = []CategoryRef{}
		}
		if rr, ok := ratingMap[id]; ok && rr.Avg.Valid {
			films[i].RatingAvg = roundFloat(rr.Avg.Float64, 2)
			films[i].ReviewCount = rr.Cnt
		} else if ok {
			films[i].RatingAvg = 0
			films[i].ReviewCount = rr.Cnt
		} else {
			films[i].RatingAvg = 0
			films[i].ReviewCount = 0
		}
	}

	return FilmsPage{
		Page:  page,
		Size:  len(films),
		Total: total,
		Films: films,
	}, nil
}

func (r *filmRepoSQL) GetFilmByID(ctx context.Context, id int) (*FilmListItem, error) {
	query := `
		SELECT f.id, f.name, f.duration, f.year_of_issue, f.age, f.link_img, f.link_kinopoisk, f.link_video, f.created_at,
		       c.id "country.id", c.name "country.name"
		FROM films f
		LEFT JOIN countries c ON c.id = f.country_id
		WHERE f.id = $1 AND f.deleted_at IS NULL
	`
	row := r.db.QueryRowxContext(ctx, query, id)

	var item FilmListItem
	var countryIDNull sql.NullInt64
	var countryName sql.NullString
	var linkImg sql.NullString
	var linkKinopoisk sql.NullString

	if err := row.Scan(
		&item.ID,
		&item.Name,
		&item.Duration,
		&item.YearOfIssue,
		&item.Age,
		&linkImg,
		&linkKinopoisk,
		&item.LinkVideo,
		&item.CreatedAt,
		&countryIDNull,
		&countryName,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("not found")
		}
		return nil, err
	}
	if linkImg.Valid {
		item.LinkImg = &linkImg.String
	}
	if linkKinopoisk.Valid {
		item.LinkKinopoisk = &linkKinopoisk.String
	}
	if countryIDNull.Valid {
		item.Country = Country{
			ID:   int(countryIDNull.Int64),
			Name: countryName.String,
		}
	}

	queryCats := `
		SELECT cat.id, cat.name
		FROM categories_films cf
		JOIN categories cat ON cat.id = cf.category_id
		WHERE cf.film_id = $1
	`
	type catRow2 struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}
	cats := []catRow2{}
	if err := r.db.SelectContext(ctx, &cats, queryCats, id); err == nil {
		for _, c := range cats {
			item.Categories = append(item.Categories, CategoryRef{ID: c.ID, Name: c.Name})
		}
	}

	var avg sql.NullFloat64
	var cnt int
	if err := r.db.GetContext(ctx, &avg, "SELECT COALESCE(AVG(ball)::numeric(10,2),0) AS avg FROM ratings WHERE film_id = $1", id); err == nil {
		_ = r.db.GetContext(ctx, &cnt, "SELECT COUNT(1) FROM ratings WHERE film_id = $1", id)
		if avg.Valid {
			item.RatingAvg = roundFloat(avg.Float64, 2)
			item.ReviewCount = cnt
		}
	}

	return &item, nil
}

func (r *genderRepoSQL) ListGenders(ctx context.Context) ([]Gender, error) {
	var genders []Gender
	if err := r.db.SelectContext(ctx, &genders, "SELECT id, name FROM gender ORDER BY id"); err != nil {
		return nil, err
	}
	return genders, nil
}

type CountryWithCount struct {
	ID        int    `db:"id" json:"id"`
	Name      string `db:"name" json:"name"`
	FilmCount int    `db:"film_count" json:"filmCount"`
}

type CategoryWithParent struct {
	ID             int          `json:"id" db:"id"`
	Name           string       `json:"name" db:"name"`
	ParentCategory *CategoryRef `json:"parentCategory,omitempty"`
	FilmCount      int          `json:"filmCount,omitempty" db:"film_count"`
}

type CountryRepository interface {
	ListCountries(ctx context.Context) ([]CountryWithCount, error)
}

type CategoryRepository interface {
	ListCategories(ctx context.Context) ([]CategoryWithParent, error)
}

type countryRepoSQL struct {
	db *sqlx.DB
}

func NewCountryRepository(db *DB) CountryRepository {
	return &countryRepoSQL{db: db.sqlx}
}

func (r *countryRepoSQL) ListCountries(ctx context.Context) ([]CountryWithCount, error) {
	query := `
		SELECT c.id, c.name, COUNT(f.id) AS film_count
		FROM countries c
		LEFT JOIN films f ON f.country_id = c.id AND f.deleted_at IS NULL
		GROUP BY c.id, c.name
		ORDER BY c.name
	`
	var out []CountryWithCount
	if err := r.db.SelectContext(ctx, &out, query); err != nil {
		return nil, err
	}
	return out, nil
}

type categoryRepoSQL struct {
	db *sqlx.DB
}

func NewCategoryRepository(db *DB) CategoryRepository {
	return &categoryRepoSQL{db: db.sqlx}
}

func (r *categoryRepoSQL) ListCategories(ctx context.Context) ([]CategoryWithParent, error) {
	query := `
		SELECT cat.id, cat.name, cat.parent_id, parent.name AS parent_name, COALESCE(COUNT(cf.film_id),0) AS film_count
		FROM categories cat
		LEFT JOIN categories parent ON parent.id = cat.parent_id
		LEFT JOIN categories_films cf ON cf.category_id = cat.id
		LEFT JOIN films f ON f.id = cf.film_id AND f.deleted_at IS NULL
		GROUP BY cat.id, cat.name, cat.parent_id, parent.name
		ORDER BY cat.name
	`
	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []CategoryWithParent
	for rows.Next() {
		var id int
		var name string
		var parentID sql.NullInt64
		var parentName sql.NullString
		var filmCount int

		if err := rows.Scan(&id, &name, &parentID, &parentName, &filmCount); err != nil {
			return nil, err
		}

		var parent *CategoryRef
		if parentID.Valid {
			parent = &CategoryRef{ID: int(parentID.Int64), Name: parentName.String}
		}

		res = append(res, CategoryWithParent{
			ID:             id,
			Name:           name,
			ParentCategory: parent,
			FilmCount:      filmCount,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func roundFloat(v float64, prec int) float64 {
	p := mathPow10(prec)
	return float64(int(v*p+0.5)) / p
}

func mathPow10(n int) float64 {
	p := 1.0
	for i := 0; i < n; i++ {
		p *= 10
	}
	return p
}

func pqArray(ids []int) []int64 {
	out := make([]int64, len(ids))
	for i, v := range ids {
		out[i] = int64(v)
	}
	return out
}
