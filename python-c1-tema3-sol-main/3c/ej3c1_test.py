"""
Tests para el ejercicio ej3c1.py que implementa autenticación con token simple.
"""

import pytest
from flask import Flask
from flask.testing import FlaskClient
from ej3c1 import create_app, API_TOKEN

@pytest.fixture
def client() -> FlaskClient:
    """Fixture que proporciona un cliente de prueba Flask"""
    app = create_app()
    app.testing = True
    return app.test_client()

def test_public_endpoint(client):
    """Prueba el endpoint público sin autenticación"""
    response = client.get('/api/public')
    assert response.status_code == 200
    assert 'Este es un endpoint público' in response.json.get('message', '')

def test_protected_endpoint_no_auth(client):
    """Prueba el endpoint protegido sin autenticación"""
    response = client.get('/api/secret')
    assert response.status_code == 401
    assert 'error' in response.json

def test_protected_endpoint_wrong_format(client):
    """Prueba el endpoint protegido con formato de autorización incorrecto"""
    response = client.get('/api/secret', headers={'Authorization': API_TOKEN})
    assert response.status_code == 401
    assert 'error' in response.json

def test_protected_endpoint_wrong_token(client):
    """Prueba el endpoint protegido con token incorrecto"""
    response = client.get('/api/secret', headers={'Authorization': f'Bearer token_incorrecto'})
    assert response.status_code == 401
    assert 'error' in response.json

def test_protected_endpoint_correct_token(client):
    """Prueba el endpoint protegido con token correcto"""
    response = client.get('/api/secret', headers={'Authorization': f'Bearer {API_TOKEN}'})
    assert response.status_code == 200
    assert 'secret' in response.json
    assert '42' in response.json.get('secret', '')
