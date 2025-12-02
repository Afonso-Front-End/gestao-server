"""
Rotas de exclusão de pedidos no galpão
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.services.database import get_database
from app.core.collections import COLLECTION_SLA_PEDIDOS_GALPAO

router = APIRouter(tags=["Pedidos Galpão - Delete"])

@router.delete("/{base_name}")
async def clear_pedidos_no_galpao(base_name: str) -> JSONResponse:
    """
    Limpa pedidos no galpão para uma base específica
    """
    try:
        db = get_database()
        
        # Contar antes da exclusão
        count_before = await db[COLLECTION_SLA_PEDIDOS_GALPAO].count_documents({"_base_name": base_name})
        
        # Deletar pedidos da base
        result = await db[COLLECTION_SLA_PEDIDOS_GALPAO].delete_many({"_base_name": base_name})
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": f"Pedidos no galpão limpos para a base {base_name}",
                "deleted_count": result.deleted_count,
                "previous_count": count_before
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao limpar pedidos no galpão: {str(e)}"
        )

@router.delete("/")
async def clear_all_pedidos_no_galpao() -> JSONResponse:
    """
    Limpa todos os pedidos no galpão
    """
    try:
        db = get_database()
        
        # Contar antes da exclusão
        count_before = await db[COLLECTION_SLA_PEDIDOS_GALPAO].count_documents({})
        
        # Deletar todos os pedidos
        result = await db[COLLECTION_SLA_PEDIDOS_GALPAO].delete_many({})
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Todos os pedidos no galpão foram limpos",
                "deleted_count": result.deleted_count,
                "previous_count": count_before
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao limpar pedidos no galpão: {str(e)}"
        )

