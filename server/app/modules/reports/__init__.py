"""
Módulo de Reports & Analytics
Gerencia snapshots, dashboards e relatórios do sistema
"""
from fastapi import APIRouter
from .routes import dashboard, snapshots

router = APIRouter(prefix="/api/reports", tags=["Reports & Analytics"])

# Incluir routers
router.include_router(snapshots.router)
router.include_router(dashboard.router)

