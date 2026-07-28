from fastapi.testclient import TestClient

from app.main import app


def test_login_page_renders() -> None:
    with TestClient(app, base_url="https://testserver") as client:
        response = client.get("/login")
    assert response.status_code == 200
    assert "Market Data Miner" in response.text


def test_invalid_login_renders_error() -> None:
    with TestClient(app, base_url="https://testserver") as client:
        response = client.post(
            "/login",
            data={"username": "incorrect", "password": "incorrect"},
        )
    assert response.status_code == 401
    assert "Incorrect username or password" in response.text
