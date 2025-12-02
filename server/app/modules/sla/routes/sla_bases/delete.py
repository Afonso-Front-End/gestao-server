"""
Rotas de exclusão de dados de bases SLA
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.modules.sla.services.sla_bases_service import SLABasesService

router = APIRouter(tags=["SLA Bases - Delete"])

# Instância do serviço
sla_bases_service = SLABasesService()

@router.delete("/data/clear-all")
async def clear_all_sla_data() -> JSONResponse:
    """
    Limpa todos os dados SLA (bases, files, chunks, galpao_entradas, pedidos_no_galpao).
    
    Coleções que serão limpas:
    - sla_bases_data
    - sla_files
    - sla_chunks
    - galpao_entradas
    - pedidos_no_galpao
    """
    try:
        from app.services.database import get_database
        
        db = get_database()
        
        from app.core.collections import (
            COLLECTION_SLA_BASES,
            COLLECTION_SLA_FILES,
            COLLECTION_SLA_CHUNKS,
            COLLECTION_SLA_GALPAO_ENTRADAS,
            COLLECTION_SLA_PEDIDOS_GALPAO
        )
        
        # Contar registros antes da exclusão
        sla_bases_count = await db[COLLECTION_SLA_BASES].count_documents({})
        sla_files_count = await db[COLLECTION_SLA_FILES].count_documents({})
        sla_chunks_count = await db[COLLECTION_SLA_CHUNKS].count_documents({})
        galpao_entradas_count = await db[COLLECTION_SLA_GALPAO_ENTRADAS].count_documents({})
        pedidos_galpao_count = await db[COLLECTION_SLA_PEDIDOS_GALPAO].count_documents({})
        
        # Deletar todos os dados SLA
        sla_result = await db[COLLECTION_SLA_BASES].delete_many({})
        sla_files_result = await db[COLLECTION_SLA_FILES].delete_many({})
        sla_chunks_result = await db[COLLECTION_SLA_CHUNKS].delete_many({})
        galpao_entradas_result = await db[COLLECTION_SLA_GALPAO_ENTRADAS].delete_many({})
        pedidos_galpao_result = await db[COLLECTION_SLA_PEDIDOS_GALPAO].delete_many({})
        
        total_deleted = (sla_result.deleted_count + 
                        sla_files_result.deleted_count + 
                        sla_chunks_result.deleted_count +
                        galpao_entradas_result.deleted_count +
                        pedidos_galpao_result.deleted_count)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Todos os dados SLA foram limpos com sucesso",
                "deleted_counts": {
                    "sla_bases": sla_result.deleted_count,
                    "sla_files": sla_files_result.deleted_count,
                    "sla_chunks": sla_chunks_result.deleted_count,
                    "galpao_entradas": galpao_entradas_result.deleted_count,
                    "pedidos_no_galpao": pedidos_galpao_result.deleted_count,
                    "total": total_deleted
                },
                "previous_counts": {
                    "sla_bases": sla_bases_count,
                    "sla_files": sla_files_count,
                    "sla_chunks": sla_chunks_count,
                    "galpao_entradas": galpao_entradas_count,
                    "pedidos_no_galpao": pedidos_galpao_count
                }
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao limpar dados SLA: {str(e)}"
        )

@router.delete("/data/{base_name}")
async def delete_base_data(base_name: str) -> JSONResponse:
    """
    Remove os dados de uma base específica
    
    Args:
        base_name: Nome da base
        
    Returns:
        JSONResponse com resultado da remoção
    """
    try:
        db = sla_bases_service._get_database()
        
        result = await db.sla_bases_data.delete_one({"base_name": base_name})
        
        if result.deleted_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Base não encontrada"
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "message": f"Dados da base {base_name} removidos com sucesso",
                "data": {
                    "base_name": base_name,
                    "deleted_count": result.deleted_count
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

