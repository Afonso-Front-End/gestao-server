"""
Router principal para rotas de Pedidos no Galpão
"""
from fastapi import APIRouter
from .consulta import router as consulta_router
from .delete import router as delete_router

router = APIRouter(prefix="/pedidos-galpao", tags=["Pedidos no Galpão"])

# Incluir todos os sub-routers
router.include_router(consulta_router)
router.include_router(delete_router)

