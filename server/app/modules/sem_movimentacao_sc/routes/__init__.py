"""
Rotas para Sem Movimentação SC
"""
from fastapi import APIRouter
from .upload import router as upload_router
from .list import router as list_router
from .delete import router as delete_router
from .move import router as move_router

router = APIRouter(prefix="/api/sem-movimentacao-sc", tags=["Sem Movimentação SC"])

router.include_router(upload_router)
router.include_router(list_router)
router.include_router(delete_router)
router.include_router(move_router)

