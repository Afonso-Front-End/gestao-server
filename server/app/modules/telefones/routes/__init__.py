"""
Rotas do módulo Lista de Telefones
Agrega todas as rotas em um único router
"""
from fastapi import APIRouter
from .upload import router as upload_router
from .listar import router as listar_router
from .motorista import router as motorista_router
from .chunks import router as chunks_router
from .exportar import router as exportar_router
from .cadastrar import router as cadastrar_router

# Router principal com prefixo e tags
router = APIRouter(prefix="/api/lista-telefones", tags=["Lista de Telefones"])

# Incluir todos os sub-routers
router.include_router(upload_router)
router.include_router(listar_router)
router.include_router(motorista_router)
router.include_router(chunks_router)
router.include_router(exportar_router)
router.include_router(cadastrar_router)
