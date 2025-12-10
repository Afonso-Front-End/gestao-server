"""
Torre de Controle - Aplicação Principal FastAPI
"""
from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
import sys
import os
from pathlib import Path

# Adicionar o diretório pai (server) ao path para imports funcionarem
SERVER_ROOT = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(SERVER_ROOT))

# Importar routers e serviços
from app.services.database import connect_to_mongo, close_mongo_connection
from app.modules.auth.routes import router as auth_router
from app.modules.retidos.routes import router as pedidos_retidos_router
from app.modules.telefones.routes import router as lista_telefones_router
from app.modules.sla.routes import router as sla_router
from app.modules.d1.routes import router as d1_router
from app.modules.sem_movimentacao_sc.routes import router as sem_movimentacao_sc_router
from app.modules.reports import router as reports_router
from app.routes.admin import router as admin_router

# Importar middlewares de segurança
from app.core.security import (
    SecurityHeadersMiddleware,
    OriginValidationMiddleware,
    IPValidationMiddleware,
    RateLimitMiddleware,
    APIKeyValidationMiddleware,
    ALLOWED_ORIGINS,
    require_localhost,
    require_api_key
)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Verificar se está em modo debug
DEBUG_MODE = os.getenv("DEBUG", "false").lower() == "true"

# Criar instância do FastAPI
app = FastAPI(
    title="Torre de Controle",
    description="Sistema de controle para gerenciamento de pedidos retidos, lista de telefones e SLA",
    version="1.0.0",
    # Desabilitar docs em produção
    docs_url="/docs" if DEBUG_MODE else None,
    redoc_url="/redoc" if DEBUG_MODE else None,
    openapi_url="/openapi.json" if DEBUG_MODE else None,
)

# ============================================
# Middlewares de Segurança (ordem importa!)
# ============================================

# 1. Security Headers (primeiro)
app.add_middleware(SecurityHeadersMiddleware)

# 2. Validação de IP (segundo)
app.add_middleware(IPValidationMiddleware)

# 3. Validação de Origem (terceiro)
app.add_middleware(OriginValidationMiddleware)

# 4. Validação de API Key (quarto)
app.add_middleware(APIKeyValidationMiddleware)

# 5. Rate Limiting (quinto)
app.add_middleware(RateLimitMiddleware)

# 6. CORS (último, mas restritivo)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,  # Apenas localhost
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],  # Métodos específicos
    allow_headers=["Content-Type", "Authorization", "Accept", "X-API-Key", "X-API-Secret"],  # Headers específicos + API Key
    expose_headers=["Content-Type", "Content-Length"],
    max_age=3600,  # Cache de preflight por 1 hora
)

# Registrar routers
app.include_router(auth_router)  # Rotas de autenticação (sem proteção de API Key)
app.include_router(pedidos_retidos_router)
app.include_router(lista_telefones_router)
app.include_router(reports_router)
app.include_router(sla_router)
app.include_router(d1_router)
app.include_router(sem_movimentacao_sc_router)

# Rota admin (sem proteção de admin, apenas autenticação JWT normal)
app.include_router(admin_router)

# Event handlers
@app.on_event("startup")
async def startup_event():
    """Executado ao iniciar a aplicação"""
    try:
        port = int(os.getenv("PORT", "8001"))
        host = os.getenv("HOST", "0.0.0.0")
        logger.info("🚀 Iniciando Torre de Controle...")
        await connect_to_mongo()
        logger.info("✅ Conexão com MongoDB estabelecida")
        logger.info("✅ Aplicação iniciada com sucesso")
        if DEBUG_MODE:
            logger.info(f"📚 Documentação disponível em: http://{host if host != '0.0.0.0' else 'localhost'}:{port}/docs")
        else:
            logger.info("🔒 Modo produção: Documentação desabilitada")
        logger.info("🛡️ Segurança: Apenas conexões de localhost permitidas")
        
        # Verificar se API Key está configurada
        from app.core.security import API_SECRET_KEY, DEFAULT_DEV_SECRET_KEY
        if API_SECRET_KEY and API_SECRET_KEY.strip() != "":
            logger.info("🔐 Autenticação por API Key: ATIVADA (chave customizada)")
        else:
            logger.warning("⚠️ Usando chave padrão de DESENVOLVIMENTO")
            logger.warning("⚠️ Configure API_SECRET_KEY no .env para produção!")
            logger.info("🔐 Autenticação por API Key: ATIVADA (chave padrão de dev)")
    except Exception as e:
        logger.error(f"❌ Erro ao iniciar aplicação: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Executado ao encerrar a aplicação"""
    try:
        logger.info("🛑 Encerrando aplicação...")
        await close_mongo_connection()
        logger.info("✅ Conexão com MongoDB fechada")
    except Exception as e:
        logger.error(f"❌ Erro ao encerrar aplicação: {e}")

# Middleware de tratamento de erros
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Tratamento global de exceções"""
    logger.error(f"❌ Erro não tratado: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": "Erro interno do servidor",
            "detail": str(exc) if os.getenv("DEBUG", "false").lower() == "true" else "Erro interno"
        }
    )

# Rota raiz
@app.get("/")
async def root():
    """Rota raiz - informações da API"""
    return {
        "message": "Torre de Controle API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "ok"
    }

# Rota de health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        from app.services.database import db
        if db.client is None or db.database is None:
            return JSONResponse(
                status_code=503,
                content={"status": "unhealthy", "database": "disconnected"}
            )
        # Testar conexão
        await db.client.admin.command('ping')
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "database": "error", "error": str(e)}
        )

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8001"))
    host = os.getenv("HOST", "0.0.0.0")
    uvicorn.run("app.main:app", host=host, port=port, reload=True)

