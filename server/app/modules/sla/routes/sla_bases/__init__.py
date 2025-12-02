"""
Router principal para rotas de SLA Bases
"""
from fastapi import APIRouter
from .process import router as process_router
from .stats import router as stats_router
from .data import router as data_router
from .delete import router as delete_router

router = APIRouter(prefix="/bases", tags=["SLA Bases"])

# Incluir todos os sub-routers
router.include_router(process_router)
router.include_router(stats_router)
router.include_router(data_router)
router.include_router(delete_router)

