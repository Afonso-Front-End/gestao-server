"""
Rotas de estatísticas de bases SLA
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.modules.sla.services.sla_bases_service import SLABasesService

router = APIRouter(tags=["SLA Bases - Stats"])

# Instância do serviço
sla_bases_service = SLABasesService()

@router.get("/stats/{base_name}")
async def get_base_stats(base_name: str) -> JSONResponse:
    """
    Obtém estatísticas de uma base específica
    
    Args:
        base_name: Nome da base
        
    Returns:
        JSONResponse com estatísticas da base
    """
    try:
        stats = await sla_bases_service.get_base_stats(base_name)
        
        if "error" in stats:
            raise HTTPException(
                status_code=404,
                detail=stats["error"]
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "message": "Estatísticas da base obtidas com sucesso",
                "data": stats
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno do servidor: {str(e)}"
        )

@router.get("/list")
async def get_all_processed_bases() -> JSONResponse:
    """
    Lista todas as bases processadas
    
    Returns:
        JSONResponse com lista de bases processadas
    """
    try:
        db = sla_bases_service._get_database()
        
        # Buscar todas as bases processadas
        bases = await db.sla_bases_data.find(
            {},
            {
                "base_name": 1,
                "total_records": 1,
                "total_pedidos": 1,
                "updated_at": 1,
                "status": 1,
                "_id": 0
            }
        ).to_list(None)
        
        # Converter datetime para string
        for base in bases:
            if base.get("updated_at"):
                base["last_processed"] = base["updated_at"].isoformat()
                del base["updated_at"]
        
        return JSONResponse(
            status_code=200,
            content={
                "message": "Bases processadas obtidas com sucesso",
                "data": bases
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno do servidor: {str(e)}"
        )

@router.get("/stats")
async def get_all_bases_stats() -> JSONResponse:
    """
    Obtém estatísticas de todas as bases processadas
    
    Returns:
        JSONResponse com estatísticas globais
    """
    try:
        stats = await sla_bases_service.get_all_bases_stats()
        
        if "error" in stats:
            raise HTTPException(
                status_code=500,
                detail=stats["error"]
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "message": "Estatísticas globais obtidas com sucesso",
                "data": stats
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno do servidor: {str(e)}"
        )

