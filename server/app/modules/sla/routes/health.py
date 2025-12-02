"""
Rotas de health check SLA
"""
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(tags=["SLA - Health"])

@router.get("/health")
async def health_check() -> JSONResponse:
    """
    Endpoint de health check para SLA
    
    Returns:
        JSONResponse com status do serviço
    """
    return JSONResponse(
        status_code=200,
        content={
            "message": "SLA service is healthy",
            "status": "ok",
            "service": "SLA Processor"
        }
    )

