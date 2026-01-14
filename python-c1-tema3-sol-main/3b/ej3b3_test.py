import pytest
from flask import Flask
from flask.testing import FlaskClient
from ej3b3 import create_app, db, Author, Book
import os

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

        yield app.test_client()  # Provide the test client

        # Clean up after test
        db.session.remove()
        db.drop_all()

# Tests for schema validation in POST endpoints

def test_add_author_valid(client):
    """Test POST /authors with valid data"""
    response = client.post("/authors", json={"name": "Ernest Hemingway"})
    assert response.status_code == 201
    assert response.json["name"] == "Ernest Hemingway"

def test_add_author_invalid_missing_name(client):
    """Test POST /authors with invalid data - missing required field"""
    response = client.post("/authors", json={})
    assert response.status_code == 400
    assert "error" in response.json

def test_add_author_invalid_wrong_type(client):
    """Test POST /authors with invalid data - wrong data type"""
    response = client.post("/authors", json={"name": 123})  # Name should be string
    assert response.status_code == 400
    assert "error" in response.json

def test_add_author_invalid_extra_field(client):
    """Test POST /authors with invalid data - extra field not in schema"""
    response = client.post("/authors", json={"name": "Ernest Hemingway", "age": 61})
    assert response.status_code == 400
    assert "error" in response.json

def test_add_book_valid(client):
    """Test POST /books with valid data"""
    response = client.post("/books", json={
        "title": "El coronel no tiene quien le escriba",
        "author_id": 1,
        "year": 1961
    })
    assert response.status_code == 201
    assert response.json["title"] == "El coronel no tiene quien le escriba"
    assert response.json["year"] == 1961
    assert response.json["author_id"] == 1

def test_add_book_valid_without_optional(client):
    """Test POST /books with valid data but missing optional field"""
    response = client.post("/books", json={
        "title": "Noticia de un secuestro",
        "author_id": 1
    })
    assert response.status_code == 201
    assert response.json["title"] == "Noticia de un secuestro"
    assert response.json["author_id"] == 1

def test_add_book_invalid_missing_title(client):
    """Test POST /books with invalid data - missing required field"""
    response = client.post("/books", json={"author_id": 1})
    assert response.status_code == 400
    assert "error" in response.json

def test_add_book_invalid_missing_author_id(client):
    """Test POST /books with invalid data - missing required field"""
    response = client.post("/books", json={"title": "Test Book"})
    assert response.status_code == 400
    assert "error" in response.json

def test_add_book_invalid_wrong_type(client):
    """Test POST /books with invalid data - wrong data type"""
    response = client.post("/books", json={
        "title": "Test Book",
        "author_id": "not a number",  # Should be integer
        "year": 2000
    })
    assert response.status_code == 400
    assert "error" in response.json

def test_add_book_invalid_year_range(client):
    """Test POST /books with invalid data - year out of range"""
    response = client.post("/books", json={
        "title": "Test Book",
        "author_id": 1,
        "year": 999  # Too low (schema requires >= 1000)
    })
    assert response.status_code == 400
    assert "error" in response.json

def test_add_book_with_nonexistent_author(client):
    """Test POST /books with a non-existent author"""
    response = client.post("/books", json={
        "title": "Test Book",
        "author_id": 999,
        "year": 2000
    })
    # This should fail with a 404 because the author doesn't exist
    assert response.status_code == 404
    assert "error" in response.json
