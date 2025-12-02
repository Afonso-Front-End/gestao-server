"""
Rotas de bases SLA
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.modules.sla.services.sla_processor import SLAProcessor

router = APIRouter(tags=["SLA - Bases"])

# Instância do processador
sla_processor = SLAProcessor()

@router.get("/bases")
async def get_unique_bases() -> JSONResponse:
    """
    Obtém todas as bases únicas de todos os arquivos SLA
    
    Returns:
        JSONResponse com lista de bases únicas
    """
    try:
        bases = await sla_processor.get_all_unique_bases()
        
        if "error" in bases:
            raise HTTPException(
                status_code=500,
                detail=bases["error"]
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "message": "Bases únicas obtidas com sucesso",
                "data": {
                    "bases": bases["bases"],
                    "total": len(bases["bases"])
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno do servidor: {str(e)}"
        )

