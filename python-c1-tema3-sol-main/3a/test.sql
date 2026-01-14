-- Database schema for testing
-- This file contains the schema and test data for the biblioteca database

-- Create tables
CREATE TABLE autores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL
);

CREATE TABLE libros (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    titulo TEXT NOT NULL,
    anio INTEGER,
    autor_id INTEGER,
    FOREIGN KEY (autor_id) REFERENCES autores (id)
);

-- Insert test data for authors
INSERT INTO autores (nombre) VALUES ('Gabriel García Márquez');
INSERT INTO autores (nombre) VALUES ('Isabel Allende');
INSERT INTO autores (nombre) VALUES ('Jorge Luis Borges');

-- Insert test data for books
INSERT INTO libros (titulo, anio, autor_id) VALUES ('Cien años de soledad', 1967, 1);
INSERT INTO libros (titulo, anio, autor_id) VALUES ('El amor en los tiempos del cólera', 1985, 1);
INSERT INTO libros (titulo, anio, autor_id) VALUES ('La casa de los espíritus', 1982, 2);
INSERT INTO libros (titulo, anio, autor_id) VALUES ('Paula', 1994, 2);
INSERT INTO libros (titulo, anio, autor_id) VALUES ('Ficciones', 1944, 3);
INSERT INTO libros (titulo, anio, autor_id) VALUES ('El Aleph', 1949, 3);
