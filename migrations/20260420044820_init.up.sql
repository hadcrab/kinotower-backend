CREATE TABLE IF NOT EXISTS gender (
    id SERIAL PRIMARY KEY,
    name VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS countries (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    parent_id INT,
    FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL,
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    fio VARCHAR(50) NOT NULL,
    country_id INT,
    FOREIGN KEY (country_id) REFERENCES countries(id) ON DELETE SET NULL,
    birthday DATE NOT NULL,
    gender_id INT,
    FOREIGN KEY (gender_id) REFERENCES gender(id) ON DELETE SET NULL,
    email VARCHAR(50) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS films (
    id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    country_id INT,
    FOREIGN KEY (country_id) REFERENCES countries(id) ON DELETE SET NULL,
    duration INT NOT NULL CHECK (duration > 0),
    year_of_issue INT NOT NULL CHECK (year_of_issue >= 1888),
    age INT NOT NULL CHECK (age >= 0),
    link_img VARCHAR(255) NULL,
    link_kinopoisk VARCHAR(255) NULL,
    link_video VARCHAR(255) NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS categories_films (
    id SERIAL PRIMARY KEY,
    category_id INT,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE,
    film_id INT,
    FOREIGN KEY (film_id) REFERENCES films(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ratings (
    id SERIAL PRIMARY KEY,
    user_id INT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    film_id INT,
    FOREIGN KEY (film_id) REFERENCES films(id) ON DELETE CASCADE,
    ball INT NOT NULL CHECK (ball >= 0),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reviews (
    id SERIAL PRIMARY KEY,
    user_id INT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    film_id INT,
    FOREIGN KEY (film_id) REFERENCES films(id) ON DELETE CASCADE,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    is_approved BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ
);

INSERT INTO gender (name) VALUES ('Мужской'), ('Женский'), ('Другой');

INSERT INTO countries (name) VALUES ('Россия'), ('США'), ('Франция'), ('Германия'), ('Япония'), ('Индия'), ('Китай'), ('Великобритания'), ('Италия'), ('Испания');

INSERT INTO categories (name) VALUES ('Боевик'), ('Комедия'), ('Драма'), ('Ужасы'), ('Фантастика'), ('Приключения'), ('Триллер'), ('Мультфильм'), ('Документальный'), ('Анимация');

INSERT INTO users (fio, country_id, birthday, gender_id, email, password) VALUES 
('Иван Иванов', 1, '1990-01-01', 1, 'ivan@example.com', 'hashedpassword1'),
('Мария Петрова', 2, '1985-05-15', 2, 'maria@example.com', 'hashedpassword2'),
('Алексей Сидоров', 1, '1992-03-22', 1, 'alexey@example.com', 'hashedpassword3'),
('Екатерина Морозова', 3, '1988-07-10', 2, 'ekaterina@example.com', 'hashedpassword4'),
('Джон Смит', 2, '1995-11-30', 1, 'john@example.com', 'hashedpassword5'),
('Елена Воробьева', 4, '1991-09-18', 2, 'elena@example.com', 'hashedpassword6');

INSERT INTO films (name, country_id, duration, year_of_issue, age, link_img, link_kinopoisk, link_video) VALUES 
('Сталкер', 1, 163, 1979, 12, 'https://kinopoisk.ru/film/43/', 'https://kinopoisk.ru/film/43/', 'https://example.com/stalker.mp4'),
('Война и мир', 1, 507, 1966, 12, 'https://kinopoisk.ru/film/39/', 'https://kinopoisk.ru/film/39/', 'https://example.com/war-peace.mp4'),
('Зеркало', 1, 105, 1975, 12, 'https://kinopoisk.ru/film/42/', 'https://kinopoisk.ru/film/42/', 'https://example.com/mirror.mp4'),
('The Shawshank Redemption', 2, 142, 1994, 16, 'https://kinopoisk.ru/film/448/', 'https://kinopoisk.ru/film/448/', 'https://example.com/shawshank.mp4'),
('Inception', 2, 148, 2010, 12, 'https://kinopoisk.ru/film/23230/', 'https://kinopoisk.ru/film/23230/', 'https://example.com/inception.mp4'),
('The Matrix', 2, 136, 1999, 16, 'https://kinopoisk.ru/film/302/', 'https://kinopoisk.ru/film/302/', 'https://example.com/matrix.mp4'),
('Интерстеллар', 1, 169, 2014, 12, 'https://kinopoisk.ru/film/258687/', 'https://kinopoisk.ru/film/258687/', 'https://example.com/interstellar.mp4'),
('Темный рыцарь', 2, 152, 2008, 16, 'https://kinopoisk.ru/film/39282/', 'https://kinopoisk.ru/film/39282/', 'https://example.com/dark-knight.mp4'),
('Spirited Away', 6, 125, 2001, 6, 'https://kinopoisk.ru/film/8411/', 'https://kinopoisk.ru/film/8411/', 'https://example.com/spirited-away.mp4'),
('Pulp Fiction', 2, 154, 1994, 18, 'https://kinopoisk.ru/film/342/', 'https://kinopoisk.ru/film/342/', 'https://example.com/pulp-fiction.mp4'),
('Parasite', 8, 132, 2019, 16, 'https://kinopoisk.ru/film/1000001/', 'https://kinopoisk.ru/film/1000001/', 'https://example.com/parasite.mp4');

INSERT INTO categories_films (category_id, film_id) VALUES 
(1, 1), 
(2, 1), 
(3, 2), 
(4, 2);

INSERT INTO ratings (user_id, film_id, ball) VALUES 
(1, 1, 8), 
(2, 1, 7), 
(1, 2, 9), 
(2, 2, 6);

INSERT INTO reviews (user_id, film_id, message, is_approved) VALUES 
(1, 1, 'Отличный фильм!', TRUE), 
(2, 1, 'Мне понравилось.', TRUE), 
(4, 2, 'Очень интересный сюжет.', FALSE), 
(3, 2, 'Не мое.', TRUE),
(1, 3, 'Шедевр советского кинематографа! Глубокий философский смысл.', TRUE),
(5, 3, 'Слишком медленно, но интересно.', TRUE),
(4, 4, 'Best movie ever! Шоушенк - это класс!', TRUE),
(6, 4, 'История дружбы, которая вдохновляет.', TRUE),
(1, 5, 'Inception просто охватывает разум! Потрясающий сюжет.', TRUE),
(2, 5, 'Сложно, но стоит пересмотреть несколько раз.', TRUE),
(3, 6, 'Матрица изменила кинематограф. Просто легенда!', TRUE),
(4, 6, 'Визуальные эффекты невероятные для 1999 года.', FALSE),
(5, 7, 'Amélie - это любовь с первого кадра. Чудесная история!', FALSE),
(6, 7, 'Французские фильмы знают, как тронуть душу.', FALSE),
(1, 8, 'Интерстеллар - величайший научно-фантастический фильм!', TRUE),
(2, 8, 'Эмоциональный и интеллектуальный одновременно.', TRUE),
(3, 9, 'Темный рыцарь - вершина супергеройского кино.', FALSE),
(4, 9, 'Джокер Хита Леджера - просто гений актерства.', TRUE),
(5, 10, 'Унесенные волшебством - шедевр аниме!', TRUE),
(6, 10, 'Потрясающие визуальные эффекты и история.', TRUE),
(1, 11, 'Pulp Fiction - культовый фильм для киноманов.', TRUE),
(2, 11, 'Нелинейный сюжет работает идеально!', FALSE);

INSERT INTO categories_films (category_id, film_id) VALUES 
(1, 3), 
(2, 3), 
(3, 4), 
(4, 4), 
(5, 5), 
(6, 6), 
(7, 7), 
(8, 8), 
(9, 9), 
(10, 10), 
(1, 11);