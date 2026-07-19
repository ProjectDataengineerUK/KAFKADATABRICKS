"""API FastAPI do pipeline de consentimento (banco -> seguradora -> Susep)."""

from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm

from api.auth import create_access_token, validar_credenciais_demo
from api.models import Token
from api.routers.consentimentos import router as consentimentos_router
from api.routers.gold import router as gold_router
from api.routers.status import router as status_router

app = FastAPI(
    title="Consent Pipeline API",
    description="API de consulta ao histórico de consentimento de clientes (base Susep simulada).",
    version="1.0.0",
)

app.include_router(consentimentos_router)
app.include_router(status_router)
app.include_router(gold_router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/token", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends()) -> Token:
    if not validar_credenciais_demo(form_data.username, form_data.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuário ou senha inválidos",
        )
    token, expires_in_minutes = create_access_token(subject=form_data.username)
    return Token(access_token=token, expires_in_minutes=expires_in_minutes)
