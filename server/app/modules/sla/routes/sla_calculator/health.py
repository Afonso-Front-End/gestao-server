"""
Rotas de health check do SLA Calculator
"""
from fastapi import APIRouter

router = APIRouter(tags=["SLA Calculator - Health"])

@router.get("/health")
async def health_check():
    """Verifica se o serviço de cálculo está funcionando"""
    return {
        "status": "healthy",
        "service": "SLA Calculator",
        "message": "Serviço de cálculo de SLA funcionando normalmente"
    }

