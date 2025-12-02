"""
Router principal para rotas de SLA Calculator
"""
from fastapi import APIRouter
from .metrics import router as metrics_router
from .cities import router as cities_router
from .pedidos import router as pedidos_router
from .health import router as health_router

router = APIRouter(prefix="/calculator", tags=["SLA Calculator"])

# Incluir todos os sub-routers
router.include_router(metrics_router)
router.include_router(cities_router)
router.include_router(pedidos_router)
router.include_router(health_router)

