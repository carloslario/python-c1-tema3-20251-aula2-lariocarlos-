"""
Enunciado:
En este ejercicio aprenderás a utilizar MongoDB con Python para trabajar
con bases de datos NoSQL. MongoDB es una base de datos orientada a documentos que
almacena datos en formato similar a JSON (BSON).

Tareas:
1. Conectar a una base de datos MongoDB
2. Crear colecciones (equivalentes a tablas en SQL)
3. Insertar, actualizar, consultar y eliminar documentos
4. Manejar transacciones y errores

Este ejercicio se enfoca en las operaciones básicas de MongoDB desde Python utilizando PyMongo.
"""

import subprocess
import time
import os
import sys
from typing import List, Tuple, Optional

import pymongo
from bson.objectid import ObjectId

# Configuración de MongoDB (la debes obtener de "docker-compose.yml"):
DB_NAME = "biblioteca"
MONGODB_PORT = 27017
MONGODB_HOST = "localhost"
MONGODB_USERNAME = "testuser"
MONGODB_PASSWORD = "testpass"

def verificar_docker_instalado() -> bool:
    """
    Verifica si Docker está instalado en el sistema y el usuario tiene permisos
    """
    try:
        # Verificar si docker está instalado
        result = subprocess.run(["docker", "--version"],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               text=True)
        if result.returncode != 0:
            return False

        # Verificar si docker-compose está instalado
        result = subprocess.run(["docker", "compose", "version"],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               text=True)
        if result.returncode != 0:
            return False

        # Verificar permisos de Docker
        result = subprocess.run(["docker", "ps"],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

def iniciar_mongodb_docker() -> bool:
    """
    Inicia MongoDB usando Docker Compose
    """
    try:
        # Obtener la ruta al directorio actual donde está el docker-compose.yml
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Detener cualquier contenedor previo
        subprocess.run(
            ["docker", "compose", "down"],
            cwd=current_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )

        # Iniciar MongoDB con docker-compose
        result = subprocess.run(
            ["docker", "compose", "up", "-d"],
            cwd=current_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        if result.returncode != 0:
            print(f"Error al iniciar MongoDB: {result.stderr}")
            return False

        # Dar tiempo para que MongoDB se inicie completamente
        time.sleep(5)
        return True

    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar Docker Compose: {e}")
        return False
    except Exception as e:
        print(f"Error inesperado: {e}")
        return False

def detener_mongodb_docker() -> None:
    """
    Detiene el contenedor de MongoDB
    """
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        subprocess.run(
            ["docker", "compose", "down"],
            cwd=current_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
    except Exception as e:
        print(f"Error al detener MongoDB: {e}")

def crear_conexion() -> pymongo.database.Database:
    """
    Crea y devuelve una conexión a la base de datos MongoDB
    """
    # Debes conectarte a la base de datos MongoDB usando PyMongo
    client = pymongo.MongoClient(
        f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}/",
        serverSelectionTimeoutMS=5000  # 5 segundos de timeout
    )

    try:
        # Verificar la conexión
        client.admin.command('ping')
    except Exception as e:
        print(f"No se pudo conectar a MongoDB: {e}")
        raise

    # Obtener o crear la base de datos
    db = client[DB_NAME]
    return db

def crear_colecciones(db: pymongo.database.Database) -> None:
    """
    Crea las colecciones necesarias para la biblioteca.
    En MongoDB, no es necesario definir el esquema de antemano,
    pero podemos crear índices para optimizar el rendimiento.
    """
    # Debes crear colecciones para 'autores' y 'libros'
    # 1. Crear colección de autores con índice por nombre
    db.autores.create_index("nombre", unique=True)

    # 2. Crear colección de libros con índices
    db.libros.create_index([("titulo", pymongo.ASCENDING)])
    db.libros.create_index([("autor_id", pymongo.ASCENDING)])
    db.libros.create_index([("anio", pymongo.ASCENDING)])

def insertar_autores(db: pymongo.database.Database, autores: List[Tuple[str]]) -> List[str]:
    """
    Inserta varios autores en la colección 'autores'
    """
    # Debes realizar los siguientes pasos:
    # 1. Convertir las tuplas a documentos
    docs_autores = [{"nombre": autor[0]} for autor in autores]

    # 2. Insertar los documentos
    resultado = db.autores.insert_many(docs_autores)

    # 3. Devolver los IDs como strings
    return [str(id) for id in resultado.inserted_ids]

def insertar_libros(db: pymongo.database.Database, libros: List[Tuple[str, int, str]]) -> List[str]:
    """
    Inserta varios libros en la colección 'libros'
    """
    # Debes realizar los siguientes pasos:
    # 1. Convertir las tuplas a documentos
    docs_libros = [
        {
            "titulo": libro[0],
            "anio": libro[1],
            "autor_id": ObjectId(libro[2]) if isinstance(libro[2], str) else libro[2]
        }
        for libro in libros
    ]

    # 2. Insertar los documentos
    resultado = db.libros.insert_many(docs_libros)

    # 3. Devolver los IDs como strings
    return [str(id) for id in resultado.inserted_ids]

def consultar_libros(db: pymongo.database.Database) -> None:
    """
    Consulta todos los libros y muestra título, año y nombre del autor
    """
    # Debes realizar los siguientes pasos:
    # 1. Realizar una agregación para unir libros con autores
    pipeline = [
        {
            "$lookup": {
                "from": "autores",
                "localField": "autor_id",
                "foreignField": "_id",
                "as": "autor"
            }
        },
        {
            "$unwind": "$autor"
        },
        {
            "$project": {
                "titulo": 1,
                "anio": 1,
                "autor_nombre": "$autor.nombre"
            }
        },
        {
            "$sort": {"autor_nombre": 1, "titulo": 1}
        }
    ]

    resultados = db.libros.aggregate(pipeline)

    # 2. Mostrar los resultados
    for libro in resultados:
        print(f"{libro['titulo']} ({libro['anio']}) - {libro['autor_nombre']}")

def buscar_libros_por_autor(db: pymongo.database.Database, nombre_autor: str) -> List[Tuple[str, int]]:
    """
    Busca libros por el nombre del autor
    """
    # Debes realizar los siguientes pasos:
    # 1. Primero encontrar el autor
    autor = db.autores.find_one({"nombre": nombre_autor})
    if not autor:
        return []

    # Buscar todos los libros del autor
    pipeline = [
        {
            "$match": {"autor_id": autor["_id"]}
        },
        {
            "$sort": {"anio": 1}
        },
        {
            "$project": {
                "titulo": 1,
                "anio": 1,
                "_id": 0
            }
        }
    ]

    libros = list(db.libros.aggregate(pipeline))

    # 2. Convertir a lista de tuplas (titulo, anio)
    return [(libro["titulo"], libro["anio"]) for libro in libros]

def actualizar_libro(
        db: pymongo.database.Database,
        id_libro: str,
        nuevo_titulo: Optional[str]=None,
        nuevo_anio: Optional[int]=None
) -> bool:
    """
    Actualiza la información de un libro existente
    """
    # Debes realizar los siguientes pasos:
    # 1. Crear diccionario de actualización
    actualizacion = {}
    if nuevo_titulo is not None:
        actualizacion["titulo"] = nuevo_titulo
    if nuevo_anio is not None:
        actualizacion["anio"] = nuevo_anio

    if not actualizacion:
        return True

    # 2. Realizar la actualización
    resultado = db.libros.update_one(
        {"_id": ObjectId(id_libro)},
        {"$set": actualizacion}
    )

    return resultado.modified_count > 0

def eliminar_libro(
        db: pymongo.database.Database,
        id_libro: str
) -> bool:
    """
    Elimina un libro por su ID
    """
    # Debes eliminar el libro con el ID proporcionado
    resultado = db.libros.delete_one({"_id": ObjectId(id_libro)})
    return resultado.deleted_count > 0

def ejemplo_transaccion(db: pymongo.database.Database) -> bool:
    """
    Demuestra el uso de operaciones agrupadas
    """
    # Debes realizar los siguientes pasos:
    try:
        # 1. Insertar un nuevo autor
        autor_result = db.autores.insert_one({"nombre": "Miguel de Cervantes"})
        autor_id = autor_result.inserted_id

        # 2. Insertar dos libros del autor
        libros = [
            {
                "titulo": "Don Quijote de la Mancha",
                "anio": 1605,
                "autor_id": autor_id
            },
            {
                "titulo": "Novelas ejemplares",
                "anio": 1613,
                "autor_id": autor_id
            }
        ]
        db.libros.insert_many(libros)

        return True

    except Exception as e:
        print(f"Error en la operación: {e}")
        # Intentar limpiar los datos en caso de error
        try:
            if 'autor_id' in locals():
                db.autores.delete_one({"_id": autor_id})
                db.libros.delete_many({"autor_id": autor_id})
        except:
            pass
        return False


if __name__ == "__main__":
    mongodb_proceso = None
    db = None

    try:
        # Verificar si Docker está instalado
        if not verificar_docker_instalado():
            print("Error: Docker no está instalado o no está disponible en el PATH.")
            print("Por favor, instala Docker y asegúrate de que esté en tu PATH.")
            sys.exit(1)

        # Iniciar MongoDB usando Docker
        print("Iniciando MongoDB con Docker...")
        if not iniciar_mongodb_docker():
            print("No se pudo iniciar MongoDB. Asegúrate de tener los permisos necesarios.")
            sys.exit(1)

        print("MongoDB iniciado correctamente.")

        # Crear una conexión
        print("Conectando a MongoDB...")
        db = crear_conexion()
        print("Conexión establecida correctamente.")

        # TODO: Implementar el código para probar las funciones

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Cerrar la conexión a MongoDB
        if db:
            print("\nConexión a MongoDB cerrada.")

        # Detener el proceso de MongoDB si lo iniciamos nosotros
        if mongodb_proceso:
            print("Deteniendo MongoDB...")
            detener_mongodb_docker()

            print("MongoDB detenido correctamente.")
