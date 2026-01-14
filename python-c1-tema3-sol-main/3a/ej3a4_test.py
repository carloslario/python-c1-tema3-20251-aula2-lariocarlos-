"""
Tests para el ejercicio ej3a4.py que utiliza la biblioteca PyMongo para
trabajar con bases de datos MongoDB.
"""

import pytest
import pymongo
from bson.objectid import ObjectId
from ej3a4 import (
    verificar_docker_instalado, iniciar_mongodb_docker, detener_mongodb_docker,
    crear_conexion, crear_colecciones, insertar_autores, insertar_libros,
    consultar_libros, buscar_libros_por_autor, actualizar_libro, eliminar_libro,
    ejemplo_transaccion
)

# Datos de prueba
AUTORES_PRUEBA = [
    ("Gabriel García Márquez",),
    ("Isabel Allende",),
    ("Jorge Luis Borges",)
]

# Verificar si Docker está disponible
docker_disponible = verificar_docker_instalado()

if not docker_disponible:
    pytest.skip("Docker no está instalado o el usuario no tiene permisos, omitiendo pruebas", allow_module_level=True)

@pytest.fixture(scope="session", autouse=True)
def setup_mongodb():
    """Fixture para iniciar y detener MongoDB"""
    if not iniciar_mongodb_docker():
        pytest.skip("No se pudo iniciar MongoDB en Docker")
    yield
    detener_mongodb_docker()

@pytest.fixture
def conexion():
    """Fixture para proporcionar una conexión a la base de datos"""
    try:
        db = crear_conexion()
        crear_colecciones(db)
        yield db
        # Limpiar las colecciones después de cada prueba
        db.autores.drop()
        db.libros.drop()
    except Exception as e:
        pytest.skip(f"No se pudo conectar a MongoDB: {e}")

@pytest.fixture
def datos_prueba(conexion):
    """Fixture para cargar datos de prueba"""
    # Insertar autores
    autor_ids = insertar_autores(conexion, AUTORES_PRUEBA)

    # Datos de libros
    libros = [
        ("Cien años de soledad", 1967, autor_ids[0]),
        ("El amor en los tiempos del cólera", 1985, autor_ids[0]),
        ("La casa de los espíritus", 1982, autor_ids[1]),
        ("Paula", 1994, autor_ids[1]),
        ("Ficciones", 1944, autor_ids[2]),
        ("El Aleph", 1949, autor_ids[2])
    ]

    # Insertar libros
    libro_ids = insertar_libros(conexion, libros)

    return {
        'autor_ids': autor_ids,
        'libro_ids': libro_ids
    }

def test_verificar_docker_instalado():
    """Prueba la verificación de Docker"""
    assert verificar_docker_instalado() == True

def test_crear_conexion(conexion):
    """Prueba la creación de una conexión a MongoDB"""
    assert isinstance(conexion, pymongo.database.Database)
    assert conexion.name == "biblioteca"

def test_crear_colecciones(conexion):
    """Prueba la función crear_colecciones"""
    crear_colecciones(conexion)

    # Verificar que las colecciones se han creado correctamente
    colecciones = conexion.list_collection_names()

    assert "autores" in colecciones
    assert "libros" in colecciones

    # Verificar índices
    indices_autores = conexion.autores.index_information()
    indices_libros = conexion.libros.index_information()

    assert "_id_" in indices_autores
    assert "_id_" in indices_libros

def test_insertar_autores(conexion):
    """Prueba la función insertar_autores"""
    # Insertar autores
    autor_ids = insertar_autores(conexion, AUTORES_PRUEBA)

    # Verificar que se devolvieron IDs
    assert len(autor_ids) == 3

    # Verificar que los autores se insertaron correctamente
    autores_en_db = list(conexion.autores.find().sort("_id", 1))

    assert len(autores_en_db) == 3
    assert autores_en_db[0]["nombre"] == "Gabriel García Márquez"
    assert autores_en_db[1]["nombre"] == "Isabel Allende"
    assert autores_en_db[2]["nombre"] == "Jorge Luis Borges"

    # Verificar que los IDs devueltos corresponden con los documentos insertados
    for i, autor_id in enumerate(autor_ids):
        autor = conexion.autores.find_one({"_id": ObjectId(autor_id) if isinstance(autor_id, str) else autor_id})
        assert autor is not None
        assert autor["nombre"] == AUTORES_PRUEBA[i][0]

def test_insertar_libros(conexion, datos_prueba):
    """Prueba la función insertar_libros"""
    # Verificar que los libros se insertaron correctamente
    libros_en_db = list(conexion.libros.find().sort("_id", 1))

    assert len(libros_en_db) == 6
    assert libros_en_db[0]["titulo"] == "Cien años de soledad"
    assert libros_en_db[0]["anio"] == 1967
    assert libros_en_db[1]["titulo"] == "El amor en los tiempos del cólera"
    assert libros_en_db[1]["anio"] == 1985

def test_consultar_libros(conexion, datos_prueba, capfd):
    """Prueba la función consultar_libros usando capfd para capturar la salida estándar"""
    consultar_libros(conexion)

    # Capturar la salida estándar
    salida, _ = capfd.readouterr()

    # Verificar que la salida contiene información de los libros
    assert "Cien años de soledad" in salida
    assert "Gabriel García Márquez" in salida
    assert "La casa de los espíritus" in salida
    assert "Isabel Allende" in salida
    assert "Ficciones" in salida
    assert "Jorge Luis Borges" in salida

def test_buscar_libros_por_autor(conexion, datos_prueba):
    """Prueba la función buscar_libros_por_autor"""
    # Buscar libros de Gabriel García Márquez
    libros = buscar_libros_por_autor(conexion, "Gabriel García Márquez")

    # Verificar los resultados
    assert len(libros) == 2
    titulos = [libro[0] for libro in libros]
    anios = [libro[1] for libro in libros]

    assert "Cien años de soledad" in titulos
    assert "El amor en los tiempos del cólera" in titulos
    assert 1967 in anios
    assert 1985 in anios

def test_actualizar_libro(conexion, datos_prueba):
    """Prueba la función actualizar_libro"""
    # Primero obtenemos el ID del primer libro
    primer_libro = conexion.libros.find_one({"titulo": "Cien años de soledad"})
    assert primer_libro is not None
    libro_id = str(primer_libro["_id"])

    # Actualizar el título del libro
    actualizado = actualizar_libro(conexion, libro_id,
                                  nuevo_titulo="Cien años de soledad (Edición especial)")

    # Verificar que la función devuelve True (éxito)
    assert actualizado is True

    # Verificar que el libro se actualizó correctamente
    libro_actualizado = conexion.libros.find_one({"_id": ObjectId(libro_id)})

    assert libro_actualizado["titulo"] == "Cien años de soledad (Edición especial)"
    assert libro_actualizado["anio"] == 1967  # El año no debe cambiar

    # Actualizar sólo el año
    actualizado = actualizar_libro(conexion, libro_id, nuevo_anio=2020)
    assert actualizado is True

    libro_actualizado = conexion.libros.find_one({"_id": ObjectId(libro_id)})
    assert libro_actualizado["titulo"] == "Cien años de soledad (Edición especial)"
    assert libro_actualizado["anio"] == 2020

    # Actualizar ambos campos
    actualizado = actualizar_libro(conexion, libro_id,
                                  nuevo_titulo="Título actualizado", nuevo_anio=2021)
    assert actualizado is True

    libro_actualizado = conexion.libros.find_one({"_id": ObjectId(libro_id)})
    assert libro_actualizado["titulo"] == "Título actualizado"
    assert libro_actualizado["anio"] == 2021

def test_eliminar_libro(conexion, datos_prueba):
    """Prueba la función eliminar_libro"""
    # Primero obtenemos el ID del último libro
    ultimo_libro = conexion.libros.find_one({"titulo": "El Aleph"})
    assert ultimo_libro is not None
    libro_id = str(ultimo_libro["_id"])

    # Verificar que el libro existe antes de eliminarlo
    assert conexion.libros.count_documents({"_id": ObjectId(libro_id)}) == 1

    # Verificar el total de libros antes de eliminar
    total_libros_inicial = conexion.libros.count_documents({})

    # Eliminar el libro
    eliminado = eliminar_libro(conexion, libro_id)

    # Verificar que la función devuelve True (éxito)
    assert eliminado is True

    # Verificar que el libro fue eliminado
    assert conexion.libros.count_documents({"_id": ObjectId(libro_id)}) == 0

    # Verificar que sólo se eliminó ese libro
    assert conexion.libros.count_documents({}) == total_libros_inicial - 1

def test_ejemplo_transaccion(conexion):
    """Prueba la función ejemplo_transaccion"""
    # Obtener el estado inicial de la base de datos
    libros_inicial = conexion.libros.count_documents({})

    # Ejecutar la transacción
    resultado = ejemplo_transaccion(conexion)

    # Verificar el resultado de la transacción
    assert isinstance(resultado, bool)

    # Verificar que la transacción agregó libros
    libros_final = conexion.libros.count_documents({})
    assert libros_final > libros_inicial
