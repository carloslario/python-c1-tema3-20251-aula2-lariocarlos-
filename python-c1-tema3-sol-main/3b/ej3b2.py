"""
Enunciado:
Desarrolla una API REST utilizando Flask que permita realizar operaciones básicas sobre una biblioteca
con dos modelos relacionados: Autores y Libros.
La API debe exponer los siguientes endpoints:

Autores:
1. `GET /authors`: Devuelve la lista completa de autores.
2. `POST /authors`: Agrega un nuevo autor. El cuerpo de la solicitud debe incluir un JSON con el campo "name".
3. `GET /authors/<author_id>`: Obtiene los detalles de un autor específico y su lista de libros.

Libros:
1. `GET /books`: Devuelve la lista completa de libros.
2. `POST /books`: Agrega un nuevo libro. El cuerpo de la solicitud debe incluir JSON con campos "title", "author_id", y "year" (opcional).
3. `DELETE /books/<book_id>`: Elimina un libro específico por su ID.
4. `PUT /books/<book_id>`: Actualiza la información de un libro existente. El cuerpo puede incluir "title" y/o "year".

Esta versión utiliza Flask-SQLAlchemy como ORM para persistir los datos en una base de datos SQLite.
"""

from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

# Define aquí tus modelos
# Usa los mismos modelos que en el ejercicio anterior: Author y Book

class Author(db.Model):
    """
    Modelo de autor usando SQLAlchemy ORM
    Debe tener: id, name y una relación con los libros
    """
    # Define la tabla 'authors' con:
    # - __tablename__ para especificar el nombre de la tabla
    # - id: clave primaria autoincremental
    # - name: nombre del autor (obligatorio)
    # - Una relación con los libros usando db.relationship
    __tablename__ = 'authors'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    books = db.relationship("Book", back_populates="author")

    def to_dict(self):
        """Convierte el autor a un diccionario para la respuesta JSON"""
        # Implementa este método para devolver id y name
        # No incluyas la lista de libros para evitar recursión infinita
        return {
            'id': self.id,
            'name': self.name
        }


class Book(db.Model):
    """
    Modelo de libro usando SQLAlchemy ORM
    Debe tener: id, title, year (opcional), author_id y relación con el autor
    """
    # Define la tabla 'books' con:
    # - __tablename__ para especificar el nombre de la tabla
    # - id: clave primaria autoincremental
    # - title: título del libro (obligatorio)
    # - year: año de publicación (opcional)
    # - author_id: clave foránea que relaciona con la tabla 'authors'
    __tablename__ = 'books'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String, nullable=False)
    year = db.Column(db.Integer, nullable=True)
    author_id = db.Column(db.Integer, db.ForeignKey('authors.id'), nullable=False)
    author = db.relationship("Author", back_populates="books")

    def to_dict(self):
        """Convierte el libro a un diccionario para la respuesta JSON"""
        # Implementa este método para devolver id, title, year y author_id
        return {
            'id': self.id,
            'title': self.title,
            'year': self.year,
            'author_id': self.author_id
        }


def create_app():
    """
    Crea y configura la aplicación Flask con SQLAlchemy
    """
    app = Flask(__name__)
    
    # Configuración de la base de datos SQLite en memoria
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///biblioteca.db'

    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # Inicializa la base de datos con la aplicación
    db.init_app(app)
    
    # Crea todas las tablas en la base de datos
    with app.app_context():
        db.create_all()
    
    # Endpoints de Autores
    @app.route('/authors', methods=['GET'])
    def get_authors():
        """
        Devuelve la lista completa de autores
        """
        # Implementa este endpoint:
        # - Consulta todos los autores
        # - Convierte cada autor a diccionario usando to_dict()
        # - Devuelve la lista en formato JSON
        authors = Author.query.all()
        return jsonify([author.to_dict() for author in authors])

    @app.route('/authors', methods=['POST'])
    def add_author():
        """
        Agrega un nuevo autor
        El cuerpo de la solicitud debe incluir un JSON con el campo "name"
        """
        # Implementa este endpoint:
        # - Obtiene los datos JSON de la solicitud
        # - Crea un nuevo autor con el nombre proporcionado
        # - Lo guarda en la base de datos
        # - Devuelve el autor creado con código 201
        data = request.get_json()
        author = Author(name=data['name'])
        db.session.add(author)
        db.session.commit()
        return jsonify(author.to_dict()), 201
    
    @app.route('/', methods=['GET'])
    def home():
        return "API de Biblioteca funcionando ✅", 200

    @app.route('/authors/<int:author_id>', methods=['GET'])
    def get_author(author_id):
        """
        Obtiene los detalles de un autor específico y su lista de libros
        """
        # Implementa este endpoint:
        # - Busca el autor por ID (usa get_or_404 para gestionar el error 404)
        # - Devuelve los detalles del autor y su lista de libros
        author = Author.query.get_or_404(author_id)
        author_data = author.to_dict()
        author_data['books'] = [book.to_dict() for book in author.books]
        return jsonify(author_data)

    # Endpoints de Libros
    @app.route('/books', methods=['GET'])
    def get_books():
        """
        Devuelve la lista completa de libros
        """
        # Implementa este endpoint:
        # - Consulta todos los libros
        # - Convierte cada libro a diccionario
        # - Devuelve la lista en formato JSON
        books = Book.query.all()
        return jsonify([book.to_dict() for book in books])

    @app.route('/books', methods=['POST'])
    def add_book():
        """
        Agrega un nuevo libro
        El cuerpo de la solicitud debe incluir JSON con campos "title", "author_id", y "year" (opcional)
        """
        # Implementa este endpoint:
        # - Obtiene los datos JSON de la solicitud
        # - Crea un nuevo libro con título, autor_id y año (opcional)
        # - Lo guarda en la base de datos
        # - Devuelve el libro creado con código 201
        data = request.get_json()
        author = Author.query.get_or_404(data['author_id'])  # Verify author exists
        book = Book(
            title=data['title'],
            author_id=data['author_id'],
            year=data.get('year')  # Optional field
        )
        db.session.add(book)
        db.session.commit()
        return jsonify(book.to_dict()), 201

    @app.route('/books/<int:book_id>', methods=['GET'])
    def get_book(book_id):
        """
        Obtiene un libro específico por su ID
        """
        # Implementa este endpoint:
        # - Busca el libro por ID (usa get_or_404 para gestionar el error 404)
        # - Devuelve los detalles del libro
        book = Book.query.get_or_404(book_id)
        return jsonify(book.to_dict())

    @app.route('/books/<int:book_id>', methods=['DELETE'])
    def delete_book(book_id):
        """
        Elimina un libro específico por su ID
        """
        # Implementa este endpoint:
        # - Busca el libro por ID (usa get_or_404)
        # - Elimina el libro de la base de datos
        # - Devuelve respuesta vacía con código 204
        book = Book.query.get_or_404(book_id)
        db.session.delete(book)
        db.session.commit()
        return '', 204

    @app.route('/books/<int:book_id>', methods=['PUT'])
    def update_book(book_id):
        """
        Actualiza la información de un libro existente
        El cuerpo puede incluir "title" y/o "year"
        """
        # Implementa este endpoint:
        # - Obtiene los datos JSON de la solicitud
        # - Busca el libro por ID (usa get_or_404)
        # - Actualiza los campos proporcionados (título y/o año)
        # - Guarda los cambios en la base de datos
        # - Devuelve el libro actualizado
        book = Book.query.get_or_404(book_id)
        data = request.get_json()

        if 'title' in data:
            book.title = data['title']
        if 'year' in data:
            book.year = data['year']

        db.session.commit()
        return jsonify(book.to_dict())

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
