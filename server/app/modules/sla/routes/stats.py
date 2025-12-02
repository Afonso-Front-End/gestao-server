"""
Rotas de estatísticas SLA
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.modules.sla.services.sla_processor import SLAProcessor

router = APIRouter(tags=["SLA - Stats"])

# Instância do processador
sla_processor = SLAProcessor()

@router.get("/stats/{file_id}")
async def get_file_stats(file_id: str) -> JSONResponse:
    """
    Obtém estatísticas de um arquivo processado
    
    Args:
        file_id: ID do arquivo
        
    Returns:
        JSONResponse com estatísticas do arquivo
    """
    try:
        stats = await sla_processor.get_file_stats(file_id)
        
        if "error" in stats:
            raise HTTPException(
                status_code=404,
                detail=stats["error"]
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "message": "Estatísticas obtidas com sucesso",
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

@router.get("/chunk/{file_id}/{chunk_index}")
async def get_chunk_data(file_id: str, chunk_index: int) -> JSONResponse:
    """
    Obtém dados de um chunk específico
    
    Args:
        file_id: ID do arquivo
        chunk_index: Índice do chunk
        
    Returns:
        JSONResponse com dados do chunk
    """
    try:
        chunk_data = await sla_processor.get_chunk_data(file_id, chunk_index)
        
        if "error" in chunk_data:
            raise HTTPException(
                status_code=404,
                detail=chunk_data["error"]
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "message": "Dados do chunk obtidos com sucesso",
                "data": chunk_data
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno do servidor: {str(e)}"
        )

@router.get("/global-stats")
async def get_global_stats() -> JSONResponse:
    """
    Obtém estatísticas globais do sistema SLA
    
    Returns:
        JSONResponse com estatísticas globais
    """
    try:
        stats = await sla_processor.get_global_stats()
        
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

