"""
Router principal do módulo SLA
Agrega todas as rotas em um único router
"""
from fastapi import APIRouter
from .upload import router as upload_router
from .stats import router as stats_router
from .bases import router as bases_router
from .health import router as health_router
from .galpao_upload import router as galpao_upload_router
from .sla_bases import router as sla_bases_router
from .sla_calculator import router as sla_calculator_router
from .pedidos_galpao import router as pedidos_galpao_router
from .motorista_status import router as motorista_status_router

# Router principal com prefixo e tags
router = APIRouter(prefix="/api/sla", tags=["SLA"])

# Incluir todos os sub-routers
router.include_router(upload_router)
router.include_router(stats_router)
router.include_router(bases_router)
router.include_router(health_router)
router.include_router(galpao_upload_router)
router.include_router(sla_bases_router)
router.include_router(sla_calculator_router)
router.include_router(pedidos_galpao_router)
router.include_router(motorista_status_router)
