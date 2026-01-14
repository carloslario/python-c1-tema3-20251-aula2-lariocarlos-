"""
Tests para el ejercicio ej3c2.py que implementa autenticación con JWT.
"""

import pytest
import json
from flask import Flask
from flask.testing import FlaskClient
import time
import jwt
from ej3c2 import create_app, JWT_SECRET_KEY, USER_CREDENTIALS

@pytest.fixture
def client() -> FlaskClient:
    """Fixture que proporciona un cliente de prueba Flask"""
    app = create_app()
    app.testing = True
    return app.test_client()

@pytest.fixture
def auth_token(client) -> str:
    """Fixture que genera un token JWT válido realizando login"""
    username = list(USER_CREDENTIALS.keys())[0]
    password = USER_CREDENTIALS[username]
    
    response = client.post(
        '/api/auth/login',
        data=json.dumps({"username": username, "password": password}),
        content_type='application/json'
    )
    
    if response.status_code == 200 and 'token' in response.json:
        return response.json['token']
    
    # Si falla el login, crear un token manualmente para las pruebas
    payload = {
        'sub': username,
        'iat': int(time.time()),
        'exp': int(time.time()) + 3600  # 1 hour
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm='HS256')

def test_public_endpoint(client):
    """Prueba el endpoint público sin autenticación"""
    response = client.get('/api/public')
    assert response.status_code == 200
    assert 'Este es un endpoint público' in response.json.get('message', '')

def test_login_success(client):
    """Prueba login exitoso"""
    username = list(USER_CREDENTIALS.keys())[0]
    password = USER_CREDENTIALS[username]
    
    response = client.post(
        '/api/auth/login',
        data=json.dumps({"username": username, "password": password}),
        content_type='application/json'
    )
    
    assert response.status_code == 200
    assert 'token' in response.json
    assert 'expires_at' in response.json
    
    # Verificar que el token es válido
    token = response.json['token']
    try:
        decoded = jwt.decode(token, JWT_SECRET_KEY, algorithms=['HS256'])
        assert decoded['sub'] == username
        assert 'exp' in decoded
    except jwt.InvalidTokenError:
        assert False, "El token generado no es válido"

def test_login_failure(client):
    """Prueba login fallido con credenciales incorrectas"""
    response = client.post(
        '/api/auth/login',
        data=json.dumps({"username": "usuario_falso", "password": "contraseña_falsa"}),
        content_type='application/json'
    )
    
    assert response.status_code == 401
    assert 'error' in response.json

def test_protected_endpoint_no_auth(client):
    """Prueba el endpoint protegido sin autenticación"""
    response = client.get('/api/secret')
    assert response.status_code == 401
    assert 'error' in response.json

def test_protected_endpoint_invalid_token(client):
    """Prueba el endpoint protegido con token inválido"""
    response = client.get('/api/secret', headers={'Authorization': 'Bearer invalid_token'})
    assert response.status_code == 401
    assert 'error' in response.json

def test_protected_endpoint_expired_token(client):
    """Prueba el endpoint protegido con token expirado"""
    # Crear un token expirado
    payload = {
        'sub': 'usuario_demo',
        'iat': int(time.time()) - 7200,  # 2 hours ago
        'exp': int(time.time()) - 3600   # expired 1 hour ago
    }
    expired_token = jwt.encode(payload, JWT_SECRET_KEY, algorithm='HS256')
    
    response = client.get('/api/secret', headers={'Authorization': f'Bearer {expired_token}'})
    assert response.status_code == 401
    assert 'error' in response.json

def test_protected_endpoint_valid_token(client, auth_token):
    """Prueba el endpoint protegido con token válido"""
    response = client.get('/api/secret', headers={'Authorization': f'Bearer {auth_token}'})
    assert response.status_code == 200
    assert 'secret' in response.json
    assert '42' in response.json.get('secret', '')
