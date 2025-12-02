"""
Rotas do módulo Pedidos Retidos
Agrega todas as rotas em um único router
"""
from fastapi import APIRouter
from .upload import router as upload_router
from .filtros import router as filtros_router
from .bases import router as bases_router
from .selects import router as selects_router
from .motorista import router as motorista_router
from .relatorio import router as relatorio_router
from .delete import router as delete_router
from .check import router as check_router

# Router principal com prefixo e tags
router = APIRouter(prefix="/api/retidos", tags=["retidos"])

# Incluir todos os sub-routers
router.include_router(upload_router)
router.include_router(filtros_router)
router.include_router(bases_router)
router.include_router(selects_router)
router.include_router(motorista_router)
router.include_router(relatorio_router)
router.include_router(delete_router)
router.include_router(check_router)
