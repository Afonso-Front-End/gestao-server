"""
Rotas do módulo D-1
"""
from fastapi import APIRouter
from .upload import router as upload_router
from .list import router as list_router
from .verify import router as verify_router
from .bases import router as bases_router
from .pedidos import router as pedidos_router
from .bipagens import router as bipagens_router

# Router principal com prefixo e tags
router = APIRouter(prefix="/api/d1", tags=["d1"])

# Incluir todos os sub-routers
router.include_router(upload_router)
router.include_router(list_router)
router.include_router(verify_router)
router.include_router(bases_router)
router.include_router(pedidos_router)
router.include_router(bipagens_router)

