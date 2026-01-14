import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ej3b1 import (Base, Author, Book, setup_database, create_book, get_all_books,
                  get_book_by_id, update_book, delete_book, find_books_by_author)


@pytest.fixture
def session():
    """Create an isolated in-memory database and session for testing"""
    # Use a different database for tests
    engine = create_engine('sqlite:///:memory:', echo=False)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    yield session

    # Clean up after test
    session.close()
    Base.metadata.drop_all(engine)


def test_create_book(session):
    """Test creating a new book with its author"""
    book = create_book(session, "Test Book", "Test Author", 2023)

    # Check that the book was created correctly
    assert book.title == "Test Book"
    assert book.year == 2023
    assert book.author.name == "Test Author"

    # Verify it's in the database
    db_book = session.query(Book).filter_by(title="Test Book").first()
    assert db_book is not None
    assert db_book.author.name == "Test Author"


def test_create_book_existing_author(session):
    """Test creating a book with an existing author"""
    # Create author first
    author = Author(name="Existing Author")
    session.add(author)
    session.commit()

    # Create book with same author name
    book = create_book(session, "New Book", "Existing Author", 2024)

    # Should use existing author, not create a new one
    author_count = session.query(Author).filter_by(name="Existing Author").count()
    assert author_count == 1

    # Book should be linked to the existing author
    assert book.author.id == author.id


def test_get_all_books(session):
    """Test retrieving all books"""
    # Create test data
    author1 = Author(name="Author 1")
    author2 = Author(name="Author 2")

    book1 = Book(title="Book 1", year=2021, author=author1)
    book2 = Book(title="Book 2", year=2022, author=author1)
    book3 = Book(title="Book 3", year=2023, author=author2)

    session.add_all([author1, author2, book1, book2, book3])
    session.commit()

    # Test get_all_books
    books = get_all_books(session)

    assert len(books) == 3
    assert {b.title for b in books} == {"Book 1", "Book 2", "Book 3"}
    assert {b.author.name for b in books} == {"Author 1", "Author 2"}


def test_get_book_by_id(session):
    """Test retrieving a specific book by ID"""
    # Create test data
    author = Author(name="Test Author")
    book = Book(title="Test Book", year=2025, author=author)
    session.add_all([author, book])
    session.commit()

    # Get the book's ID
    book_id = book.id

    # Test get_book_by_id
    retrieved_book = get_book_by_id(session, book_id)

    assert retrieved_book is not None
    assert retrieved_book.title == "Test Book"
    assert retrieved_book.year == 2025
    assert retrieved_book.author.name == "Test Author"


def test_get_nonexistent_book(session):
    """Test retrieving a book that doesn't exist"""
    # Try to get a book with an ID that doesn't exist
    book = get_book_by_id(session, 999)

    assert book is None


def test_update_book(session):
    """Test updating book information"""
    # Create test data
    author = Author(name="Author")
    book = Book(title="Original Title", year=2020, author=author)
    session.add_all([author, book])
    session.commit()

    book_id = book.id

    # Update the book
    updated_book = update_book(session, book_id, new_title="Updated Title", new_year=2030)

    # Check that update was successful
    assert updated_book is not None
    assert updated_book.title == "Updated Title"
    assert updated_book.year == 2030

    # Verify the change is in the database
    db_book = session.query(Book).filter_by(id=book_id).first()
    assert db_book.title == "Updated Title"
    assert db_book.year == 2030


def test_update_nonexistent_book(session):
    """Test updating a book that doesn't exist"""
    result = update_book(session, 999, new_title="New Title")

    assert result is None


def test_delete_book(session):
    """Test deleting a book"""
    # Create test data
    author = Author(name="Author")
    book = Book(title="Book to Delete", year=2024, author=author)
    session.add_all([author, book])
    session.commit()

    book_id = book.id

    # Delete the book
    delete_book(session, book_id)

    # Verify book is gone
    book = session.query(Book).filter_by(id=book_id).first()
    assert book is None

    # Author should still exist
    author = session.query(Author).filter_by(name="Author").first()
    assert author is not None


def test_find_books_by_author(session):
    """Test finding books by author name"""
    # Create test data
    author1 = Author(name="Target Author")
    author2 = Author(name="Other Author")

    book1 = Book(title="Book 1", year=2021, author=author1)
    book2 = Book(title="Book 2", year=2022, author=author1)
    book3 = Book(title="Book 3", year=2023, author=author2)

    session.add_all([author1, author2, book1, book2, book3])
    session.commit()

    # Find books by author
    books = find_books_by_author(session, "Target Author")

    assert len(books) == 2
    assert {b.title for b in books} == {"Book 1", "Book 2"}
    for book in books:
        assert book.author.name == "Target Author"
