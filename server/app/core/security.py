"""
Módulo de segurança para o servidor
"""
import os
import logging
import hmac
import hashlib
from typing import Optional, List, Tuple
from fastapi import Request, HTTPException, status, Header
from fastapi.responses import Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
import time
from collections import defaultdict

logger = logging.getLogger(__name__)

# ============================================
# Configurações de Segurança
# ============================================

# Origens permitidas (apenas localhost)
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
]

# IPs permitidos (apenas localhost)
ALLOWED_IPS = [
    "127.0.0.1",
    "localhost",
    "::1",  # IPv6 localhost
]

# Rate limiting: requisições por minuto por IP
# Aumentado para ambiente local (muitas requisições simultâneas do frontend)
# Para localhost, usar limite muito alto (praticamente desabilitado)
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "5000"))  # 5000 req/min para localhost
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))  # segundos

# API Key/Secret Key para autenticação
# Se não configurada, usar chave padrão de desenvolvimento (menos segura, mas funcional)
API_SECRET_KEY = os.getenv("API_SECRET_KEY", "")
API_KEY_HEADER = "X-API-Key"
API_SECRET_HEADER = "X-API-Secret"

# Chave padrão de desenvolvimento (apenas se não houver chave configurada)
# AVISO: Esta chave é conhecida e não deve ser usada em produção!
DEFAULT_DEV_SECRET_KEY = "dev_secret_key_do_not_use_in_production_change_this_immediately"

# Armazenamento de rate limiting (em memória)
rate_limit_store = defaultdict(list)


def is_localhost(host: str) -> bool:
    """Verifica se o host é localhost"""
    host_lower = host.lower()
    return (
        host_lower in ["localhost", "127.0.0.1", "::1"] or
        host_lower.startswith("127.") or
        host_lower.startswith("::1")
    )


def get_client_ip(request: Request) -> str:
    """Obtém o IP do cliente"""
    # Verificar headers de proxy (se houver)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # Pegar o primeiro IP da lista
        return forwarded_for.split(",")[0].strip()
    
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()
    
    # IP direto do cliente
    if request.client:
        return request.client.host
    
    return "unknown"


def validate_origin(origin: Optional[str]) -> bool:
    """Valida se a origem é permitida"""
    if not origin:
        return False
    
    # Verificar se é localhost
    origin_lower = origin.lower()
    for allowed in ALLOWED_ORIGINS:
        if origin_lower == allowed.lower():
            return True
    
    # Verificar se contém localhost
    if "localhost" in origin_lower or "127.0.0.1" in origin_lower:
        # Verificar porta
        try:
            if ":" in origin_lower:
                protocol, rest = origin_lower.split("://", 1)
                if ":" in rest:
                    host, port = rest.rsplit(":", 1)
                    if is_localhost(host) and port.isdigit():
                        return True
        except:
            pass
    
    return False


def check_rate_limit(ip: str) -> bool:
    """Verifica se o IP excedeu o limite de requisições"""
    # Desabilitar rate limit para localhost (desenvolvimento)
    if is_localhost(ip):
        return True
    
    current_time = time.time()
    
    # Limpar requisições antigas
    rate_limit_store[ip] = [
        req_time for req_time in rate_limit_store[ip]
        if current_time - req_time < RATE_LIMIT_WINDOW
    ]
    
    # Verificar limite
    if len(rate_limit_store[ip]) >= RATE_LIMIT_REQUESTS:
        logger.warning(f"Rate limit excedido para IP: {ip}")
        return False
    
    # Adicionar requisição atual
    rate_limit_store[ip].append(current_time)
    return True


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Middleware para adicionar headers de segurança"""
    
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        
        # Headers de segurança
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        
        # Content Security Policy (restritivo para localhost)
        csp = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "font-src 'self' data:; "
            "connect-src 'self' http://localhost:* http://127.0.0.1:*; "
            "frame-ancestors 'none';"
        )
        response.headers["Content-Security-Policy"] = csp
        
        # HSTS (apenas se HTTPS)
        if request.url.scheme == "https":
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        
        return response


class OriginValidationMiddleware(BaseHTTPMiddleware):
    """Middleware para validar origem das requisições"""
    
    async def dispatch(self, request: Request, call_next):
        # Permitir health check e docs sem validação de origem
        if request.url.path in ["/health", "/", "/docs", "/redoc", "/openapi.json"]:
            return await call_next(request)
        
        # Validar origem
        origin = request.headers.get("Origin")
        referer = request.headers.get("Referer")
        
        # Se não tem origem, verificar referer
        if not origin and referer:
            origin = referer.rsplit("/", 1)[0] if "/" in referer else referer
        
        # Validar origem
        if origin and not validate_origin(origin):
            logger.warning(f"Origem não permitida: {origin} de IP: {get_client_ip(request)}")
            return Response(
                content='{"error": "Origem não permitida"}',
                status_code=status.HTTP_403_FORBIDDEN,
                media_type="application/json"
            )
        
        return await call_next(request)


class IPValidationMiddleware(BaseHTTPMiddleware):
    """Middleware para validar IP do cliente"""
    
    async def dispatch(self, request: Request, call_next):
        # Permitir health check sem validação de IP
        if request.url.path in ["/health", "/"]:
            return await call_next(request)
        
        client_ip = get_client_ip(request)
        
        # Verificar se é localhost
        if not is_localhost(client_ip) and client_ip != "unknown":
            logger.warning(f"IP não permitido: {client_ip}")
            return Response(
                content='{"error": "Acesso negado"}',
                status_code=status.HTTP_403_FORBIDDEN,
                media_type="application/json"
            )
        
        return await call_next(request)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Middleware para rate limiting"""
    
    async def dispatch(self, request: Request, call_next):
        # Permitir health check sem rate limit
        if request.url.path in ["/health", "/"]:
            return await call_next(request)
        
        client_ip = get_client_ip(request)
        
        # Verificar rate limit
        if not check_rate_limit(client_ip):
            logger.warning(f"Rate limit excedido para IP: {client_ip}")
            return Response(
                content='{"error": "Muitas requisições. Tente novamente mais tarde."}',
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                media_type="application/json",
                headers={"Retry-After": str(RATE_LIMIT_WINDOW)}
            )
        
        return await call_next(request)


def require_localhost(request: Request):
    """Dependency para exigir que a requisição venha de localhost"""
    client_ip = get_client_ip(request)
    
    if not is_localhost(client_ip):
        logger.warning(f"Tentativa de acesso não localhost: {client_ip}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acesso permitido apenas de localhost"
        )
    
    return True


def validate_api_key(api_key: Optional[str] = None, api_secret: Optional[str] = None) -> bool:
    """Valida a API Key e Secret Key"""
    # Usar chave padrão de desenvolvimento se não houver chave configurada
    secret_key = API_SECRET_KEY if API_SECRET_KEY and API_SECRET_KEY.strip() != "" else DEFAULT_DEV_SECRET_KEY
    
    # Sempre exigir API Key e Secret
    if not api_key or not api_secret:
        return False
    
    # Validar usando HMAC
    try:
        # A API Key deve ser o hash HMAC-SHA256 da chave secreta
        expected_key = hmac.new(
            secret_key.encode('utf-8'),
            b'api_key',
            hashlib.sha256
        ).hexdigest()
        
        # A API Secret deve ser o hash HMAC-SHA256 da chave secreta com salt diferente
        expected_secret = hmac.new(
            secret_key.encode('utf-8'),
            b'api_secret',
            hashlib.sha256
        ).hexdigest()
        
        # Comparação segura (timing-safe)
        key_valid = hmac.compare_digest(api_key, expected_key)
        secret_valid = hmac.compare_digest(api_secret, expected_secret)
        
        return key_valid and secret_valid
    except Exception as e:
        logger.error(f"Erro ao validar API key: {e}")
        return False


def get_api_credentials(request: Request) -> Tuple[Optional[str], Optional[str]]:
    """Obtém API Key e Secret dos headers"""
    api_key = request.headers.get(API_KEY_HEADER)
    api_secret = request.headers.get(API_SECRET_HEADER)
    return api_key, api_secret


def require_api_key(request: Request):
    """Dependency para exigir API Key válida"""
    # Sempre validar API Key (segurança sempre ativa)
    api_key, api_secret = get_api_credentials(request)
    
    if not validate_api_key(api_key, api_secret):
        logger.warning(f"Tentativa de acesso com API key inválida de IP: {get_client_ip(request)}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key ou Secret inválidos",
            headers={"WWW-Authenticate": "ApiKey"}
        )
    
    return True


class APIKeyValidationMiddleware(BaseHTTPMiddleware):
    """Middleware para validar API Key em todas as requisições"""
    
    async def dispatch(self, request: Request, call_next):
        # Permitir health check, docs e rotas de autenticação sem validação de API key
        allowed_paths = ["/health", "/", "/docs", "/redoc", "/openapi.json"]
        if request.url.path in allowed_paths or request.url.path.startswith("/api/auth"):
            return await call_next(request)
        
        # Sempre validar API Key (segurança sempre ativa)
        api_key, api_secret = get_api_credentials(request)
        
        if not validate_api_key(api_key, api_secret):
            logger.warning(f"API key inválida de IP: {get_client_ip(request)}")
            return Response(
                content='{"error": "API Key ou Secret inválidos"}',
                status_code=status.HTTP_401_UNAUTHORIZED,
                media_type="application/json",
                headers={"WWW-Authenticate": "ApiKey"}
            )
        
        return await call_next(request)

