"""
Enunciado:
En este ejercicio, implementarás un sistema de autenticación básico utilizando JSON Web Tokens (JWT)
para una API REST. JWT es un estándar abierto (RFC 7519) que define una forma compacta y
autónoma para transmitir información entre partes como un objeto JSON.

Tareas:
1. Implementar autenticación mediante JWT
2. Proteger la ruta del secreto para que solo usuarios autenticados puedan acceder
3. Manejar errores de autenticación y expiración de tokens con códigos HTTP apropiados

Esta versión utiliza Flask para crear la API REST y PyJWT para trabajar con tokens JWT.
"""

import datetime
import jwt
from flask import Flask, jsonify, request
from functools import wraps

# Configuración JWT
JWT_SECRET_KEY = "clave_secreta_jwt_para_firmar_tokens"  # En producción, usar una clave segura
JWT_EXPIRATION_DELTA = datetime.timedelta(hours=1)  # Tiempo de expiración del token

# Credenciales de usuario fijas (en una aplicación real estarían en una base de datos)
USER_CREDENTIALS = {
    "usuario_demo": "password123"
}

def generate_jwt_token(username):
    """
    Genera un token JWT para un usuario

    Args:
        username: Nombre de usuario

    Returns:
        str: Token JWT generado
    """
    # TODO: Implementa este método para generar un token JWT usando la biblioteca PyJWT
    # El token debe incluir:
    # - 'sub' (subject): username
    # - 'iat' (issued at): Tiempo de emisión
    # - 'exp' (expiration): Tiempo de expiración
    # Usa JWT_SECRET_KEY para firmar el token
    now = datetime.datetime.utcnow()
    expiration = now + JWT_EXPIRATION_DELTA

    # Create the JWT payload
    payload = {
        'sub': username,
        'iat': now,
        'exp': expiration
    }

    # Generate the token
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm='HS256')

def jwt_required(func):
    """
    Decorador que verifica la autenticación mediante token JWT

    Para usar este decorador, añade @jwt_required a las funciones que requieran autenticación.
    El token debe enviarse en la cabecera 'Authorization' con formato: 'Bearer TOKEN'

    Args:
        func: Función a decorar

    Returns:
        Function: Función decorada con verificación de autenticación JWT
    """
    @wraps(func)
    def decorated_function(*args, **kwargs):
        """
        Implementa esta función para:
        1. Extraer el token JWT de la cabecera 'Authorization'
        2. Verificar que el formato sea 'Bearer TOKEN'
        3. Decodificar y verificar el token usando jwt.decode()
        4. Si hay algún error (token expirado, inválido, etc.), devolver un error apropiado
        """
        # TODO: Implementa la lógica del decorador según las instrucciones
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify({"error": "Token no proporcionado"}), 401

        try:
            # Verificar formato 'Bearer TOKEN'
            auth_type, token = auth_header.split(' ')
            if auth_type.lower() != 'bearer':
                return jsonify({"error": "Formato de autorización inválido"}), 401

            # Verificar y decodificar el token
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=['HS256'])

            # Token válido, ejecutar función
            return func(*args, **kwargs)
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token expirado"}), 401
        except (jwt.InvalidTokenError, ValueError):
            return jsonify({"error": "Token inválido"}), 401
    return decorated_function


def create_app():
    """
    Crea y configura la aplicación Flask
    """
    app = Flask(__name__)

    @app.route('/api/public', methods=['GET'])
    def public_endpoint():
        """
        Endpoint público que no requiere autenticación

        Ejemplo de uso:
            GET /api/public

        Respuesta:
            Status: 200 OK
            {
                "message": "Este es un endpoint público, cualquiera puede acceder"
            }
        """
        return jsonify({
            "message": "Este es un endpoint público, cualquiera puede acceder"
        })

    @app.route('/api/auth/login', methods=['POST'])
    def login():
        """
        Inicia sesión y genera un token JWT

        Recibe username y password en un JSON, verifica las credenciales,
        y genera un nuevo token JWT si son correctas.

        Ejemplo de uso:
            POST /api/auth/login
            Content-Type: application/json

            {
                "username": "usuario_demo",
                "password": "password123"
            }

        Respuesta exitosa:
            Status: 200 OK
            {
                "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6...",
                "expires_at": "2023-01-01T12:00:00Z"
            }

        Respuesta de error:
            Status: 401 Unauthorized
            {
                "error": "Credenciales inválidas"
            }
        """
        # TODO: Implementa este endpoint según las instrucciones
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')

        # Verificar credenciales
        if username in USER_CREDENTIALS and USER_CREDENTIALS[username] == password:
            # Generar token
            token = generate_jwt_token(username)
            expiration = datetime.datetime.utcnow() + JWT_EXPIRATION_DELTA

            return jsonify({
                'token': token,
                'expires_at': expiration.isoformat() + "Z"
            }), 200
        else:
            return jsonify({'error': 'Credenciales inválidas'}), 401

    @app.route('/api/secret', methods=['GET'])
    @jwt_required
    def protected_secret():
        """
        Endpoint protegido que requiere autenticación JWT y devuelve un mensaje secreto

        Este endpoint requiere autenticación mediante el decorador @jwt_required.

        Ejemplo de uso:
            GET /api/secret
            Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6...

        Respuesta exitosa:
            Status: 200 OK
            {
                "message": "¡Has accedido al secreto con JWT!",
                "secret": "La respuesta a la vida, el universo y todo lo demás es 42"
            }

        Respuesta de error:
            Status: 401 Unauthorized
            {
                "error": "Token inválido o ausente"
            }
        """
        return jsonify({
            'message': '¡Has accedido al secreto con JWT!',
            'secret': 'La respuesta a la vida, el universo y todo lo demás es 42'
        })

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
