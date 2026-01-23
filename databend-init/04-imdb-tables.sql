-- IMDB (JOB) Table Definitions for Databend
-- Creates tables for the Join Order Benchmark
-- Uses fully qualified table names to avoid session state issues

CREATE TABLE IF NOT EXISTS imdb.aka_name (
    id INT NOT NULL,
    person_id INT NOT NULL,
    name VARCHAR(500),
    imdb_index VARCHAR(3),
    name_pcode_cf VARCHAR(11),
    name_pcode_nf VARCHAR(11),
    surname_pcode VARCHAR(11),
    md5sum VARCHAR(65)
);

CREATE TABLE IF NOT EXISTS imdb.aka_title (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    title VARCHAR(500),
    imdb_index VARCHAR(4),
    kind_id INT NOT NULL,
    production_year INT,
    phonetic_code VARCHAR(5),
    episode_of_id INT,
    season_nr INT,
    episode_nr INT,
    note VARCHAR(500),
    md5sum VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS imdb.cast_info (
    id INT NOT NULL,
    person_id INT NOT NULL,
    movie_id INT NOT NULL,
    person_role_id INT,
    note VARCHAR(1000),
    nr_order INT,
    role_id INT NOT NULL
);

CREATE TABLE IF NOT EXISTS imdb.char_name (
    id INT NOT NULL,
    name VARCHAR(500) NOT NULL,
    imdb_index VARCHAR(2),
    imdb_id INT,
    name_pcode_nf VARCHAR(5),
    surname_pcode VARCHAR(5),
    md5sum VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS imdb.comp_cast_type (
    id INT NOT NULL,
    kind VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS imdb.company_name (
    id INT NOT NULL,
    name VARCHAR(500) NOT NULL,
    country_code VARCHAR(6),
    imdb_id INT,
    name_pcode_nf VARCHAR(5),
    name_pcode_sf VARCHAR(5),
    md5sum VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS imdb.company_type (
    id INT NOT NULL,
    kind VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS imdb.complete_cast (
    id INT NOT NULL,
    movie_id INT,
    subject_id INT NOT NULL,
    status_id INT NOT NULL
);

CREATE TABLE IF NOT EXISTS imdb.info_type (
    id INT NOT NULL,
    info VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS imdb.keyword (
    id INT NOT NULL,
    keyword VARCHAR(100) NOT NULL,
    phonetic_code VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS imdb.kind_type (
    id INT NOT NULL,
    kind VARCHAR(15)
);

CREATE TABLE IF NOT EXISTS imdb.link_type (
    id INT NOT NULL,
    link VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS imdb.movie_companies (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    company_id INT NOT NULL,
    company_type_id INT NOT NULL,
    note VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS imdb.movie_info (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    info_type_id INT NOT NULL,
    info VARCHAR(10000) NOT NULL,
    note VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS imdb.movie_info_idx (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    info_type_id INT NOT NULL,
    info VARCHAR(100) NOT NULL,
    note VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS imdb.movie_keyword (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    keyword_id INT NOT NULL
);

CREATE TABLE IF NOT EXISTS imdb.movie_link (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    linked_movie_id INT NOT NULL,
    link_type_id INT NOT NULL
);

CREATE TABLE IF NOT EXISTS imdb.name (
    id INT NOT NULL,
    name VARCHAR(500) NOT NULL,
    imdb_index VARCHAR(9),
    imdb_id INT,
    gender VARCHAR(1),
    name_pcode_cf VARCHAR(5),
    name_pcode_nf VARCHAR(5),
    surname_pcode VARCHAR(5),
    md5sum VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS imdb.person_info (
    id INT NOT NULL,
    person_id INT NOT NULL,
    info_type_id INT NOT NULL,
    info VARCHAR(10000) NOT NULL,
    note VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS imdb.role_type (
    id INT NOT NULL,
    role VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS imdb.title (
    id INT NOT NULL,
    title VARCHAR(500) NOT NULL,
    imdb_index VARCHAR(5),
    kind_id INT NOT NULL,
    production_year INT,
    imdb_id INT,
    phonetic_code VARCHAR(5),
    episode_of_id INT,
    season_nr INT,
    episode_nr INT,
    series_years VARCHAR(49),
    md5sum VARCHAR(32)
);
