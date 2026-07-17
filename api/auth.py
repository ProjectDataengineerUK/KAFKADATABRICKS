"""Autenticação JWT da API — emissão e validação de tokens.

Para fins de demo de portfólio, as credenciais válidas vêm de variáveis de
ambiente (DEMO_API_USERNAME/DEMO_API_PASSWORD) em vez de uma base de
usuários real — ver Out of Scope no DEFINE (LGPD/segurança de produção
completa não implementada).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from src.common.config import get_settings

ALGORITHM = "HS256"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def create_access_token(subject: str) -> tuple[str, int]:
    settings = get_settings()
    expire_minutes = settings.jwt_expire_minutes
    expire = datetime.now(timezone.utc) + timedelta(minutes=expire_minutes)
    token = jwt.encode(
        {"sub": subject, "exp": expire}, settings.jwt_secret, algorithm=ALGORITHM
    )
    return token, expire_minutes


def get_current_subject(token: str = Depends(oauth2_scheme)) -> str:
    settings = get_settings()
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[ALGORITHM])
    except jwt.PyJWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido"
        ) from exc
    return payload["sub"]


def validar_credenciais_demo(username: str, password: str) -> bool:
    demo_username = os.environ.get("DEMO_API_USERNAME", "demo")
    demo_password = os.environ.get("DEMO_API_PASSWORD")
    if not demo_password:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="DEMO_API_PASSWORD não configurada",
        )
    return username == demo_username and password == demo_password
