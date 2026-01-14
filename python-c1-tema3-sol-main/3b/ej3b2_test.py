import pytest
from flask import Flask
from flask.testing import FlaskClient
from ej3b2 import create_app, db, Author, Book

@pytest.fixture
def client() -> FlaskClient:
    app = create_app()
    app.testing = True

    # Create application context and set up database for testing
    with app.app_context():
        db.create_all()  # Create tables for testing

        # Add some test data
        author1 = Author(name="Gabriel García Márquez")
        author2 = Author(name="Isabel Allende")
        db.session.add_all([author1, author2])
        db.session.commit()

        # Add books
        book1 = Book(title="Cien años de soledad", year=1967, author_id=author1.id)
        book2 = Book(title="El amor en los tiempos del cólera", year=1985, author_id=author1.id)
        book3 = Book(title="La casa de los espíritus", year=1982, author_id=author2.id)
        db.session.add_all([book1, book2, book3])
        db.session.commit()

        yield app.test_client()  # Provide the test client

        # Clean up after test
        db.session.remove()
        db.drop_all()

# Tests for Author endpoints
def test_get_authors(client):
    """Test GET /authors to retrieve all authors"""
    response = client.get("/authors")
    assert response.status_code == 200
    data = response.json
    assert len(data) == 2
    assert any(author["name"] == "Gabriel García Márquez" for author in data)
    assert any(author["name"] == "Isabel Allende" for author in data)

def test_add_author(client):
    """Test POST /authors to add a new author"""
    response = client.post("/authors", json={"name": "Ernest Hemingway"})
    assert response.status_code == 201
    assert response.json["name"] == "Ernest Hemingway"

    # Verify author was added
    get_response = client.get("/authors")
    assert len(get_response.json) == 3

def test_get_author_with_books(client):
    """Test GET /authors/<id> to get author details and their books"""
    # Get author 1 (Gabriel García Márquez)
    response = client.get("/authors/1")
    assert response.status_code == 200
    data = response.json

    assert data["name"] == "Gabriel García Márquez"
    assert "books" in data
    assert len(data["books"]) == 2  # Should have 2 books
    book_titles = [book["title"] for book in data["books"]]
    assert "Cien años de soledad" in book_titles
    assert "El amor en los tiempos del cólera" in book_titles

def test_get_nonexistent_author(client):
    """Test GET /authors/<id> for a non-existent author"""
    response = client.get("/authors/999")
    assert response.status_code == 404

# Tests for Book endpoints
def test_get_books(client):
    """Test GET /books to retrieve all books"""
    response = client.get("/books")
    assert response.status_code == 200
    data = response.json
    assert len(data) == 3

    # Verify some book titles
    titles = [book["title"] for book in data]
    assert "Cien años de soledad" in titles
    assert "La casa de los espíritus" in titles

def test_get_book_by_id(client):
    """Test GET /books/<id> to retrieve a specific book"""
    response = client.get("/books/1")
    assert response.status_code == 200
    data = response.json
    assert data["title"] == "Cien años de soledad"
    assert data["year"] == 1967
    assert data["author_id"] == 1

def test_get_nonexistent_book(client):
    """Test GET /books/<id> for a non-existent book"""
    response = client.get("/books/999")
    assert response.status_code == 404

def test_add_book(client):
    """Test POST /books to add a new book"""
    response = client.post("/books", json={
        "title": "El coronel no tiene quien le escriba",
        "author_id": 1,
        "year": 1961
    })
    assert response.status_code == 201
    assert response.json["title"] == "El coronel no tiene quien le escriba"
    assert response.json["year"] == 1961
    assert response.json["author_id"] == 1

    # Verify book was added
    get_response = client.get("/books")
    assert len(get_response.json) == 4

def test_add_book_with_nonexistent_author(client):
    """Test POST /books with a non-existent author"""
    response = client.post("/books", json={
        "title": "Test Book",
        "author_id": 999,
        "year": 2025
    })
    # This should either fail with a 404 or a 400 depending on implementation
    assert response.status_code in [400, 404]

def test_update_book(client):
    """Test PUT /books/<id> to update a book"""
    # Update book 1
    response = client.put("/books/1", json={
        "title": "Cien años de soledad (Edición especial)",
        "year": 2020
    })
    assert response.status_code == 200
    assert response.json["title"] == "Cien años de soledad (Edición especial)"
    assert response.json["year"] == 2020
    assert response.json["author_id"] == 1  # Author should remain unchanged

    # Verify book was updated
    get_response = client.get("/books/1")
    assert get_response.json["title"] == "Cien años de soledad (Edición especial)"

def test_update_nonexistent_book(client):
    """Test PUT /books/<id> for a non-existent book"""
    response = client.put("/books/999", json={"title": "Non-existent Book"})
    assert response.status_code == 404

def test_delete_book(client):
    """Test DELETE /books/<id> to delete a book"""
    # Delete book 3
    response = client.delete("/books/3")
    assert response.status_code == 204

    # Verify book was deleted
    get_response = client.get("/books")
    assert len(get_response.json) == 2
    titles = [book["title"] for book in get_response.json]
    assert "La casa de los espíritus" not in titles

def test_delete_nonexistent_book(client):
    """Test DELETE /books/<id> for a non-existent book"""
    response = client.delete("/books/999")
    assert response.status_code == 404
