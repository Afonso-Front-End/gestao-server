"""
Rotas administrativas para gerenciamento do sistema
⚠️ ATENÇÃO: Estas rotas são protegidas e só funcionam de localhost
"""
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
from app.services.database import get_database
from app.core.security import require_localhost
from app.core.collections import (
    # SLA Collections
    COLLECTION_SLA_BASES,
    COLLECTION_SLA_GALPAO_ENTRADAS,
    COLLECTION_SLA_PEDIDOS_GALPAO,
    COLLECTION_SLA_FILES,
    COLLECTION_SLA_CHUNKS,
    # Pedidos Retidos Collections
    COLLECTION_PEDIDOS_RETIDOS,
    COLLECTION_PEDIDOS_RETIDOS_CHUNKS,
    COLLECTION_PEDIDOS_RETIDOS_TABELA,
    COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS,
    # Telefones (NÃO DELETAR)
    # COLLECTION_TELEFONES - Mantida intacta
)

router = APIRouter(prefix="/api/admin", tags=["Admin"])

@router.delete("/clear-all-data")
async def clear_all_collections() -> JSONResponse:
    """
    Deleta todos os dados de todas as coleções, EXCETO a coleção de telefones.
    
    Coleções que serão limpas:
    - SLA: bases, galpão, pedidos no galpão, files, chunks
    - Pedidos Retidos: todas as coleções relacionadas
    
    Coleções que NÃO serão limpas:
    - Telefones
    """
    try:
        db = get_database()
        
        if db is None:
            raise HTTPException(status_code=500, detail="Não foi possível conectar ao banco de dados")
        
        results = {}
        previous_counts = {}
        deleted_counts = {}
        
        # ========================================
        # SLA Collections
        # ========================================
        sla_collections = {
            "sla_bases": COLLECTION_SLA_BASES,
            "galpao_entradas": COLLECTION_SLA_GALPAO_ENTRADAS,
            "pedidos_no_galpao": COLLECTION_SLA_PEDIDOS_GALPAO,
            "sla_files": COLLECTION_SLA_FILES,
            "sla_chunks": COLLECTION_SLA_CHUNKS,
        }
        
        for name, collection_name in sla_collections.items():
            try:
                previous_counts[name] = await db[collection_name].count_documents({})
                result = await db[collection_name].delete_many({})
                deleted_counts[name] = result.deleted_count
                results[name] = {
                    "success": True,
                    "deleted": result.deleted_count,
                    "previous": previous_counts[name]
                }
            except Exception as e:
                results[name] = {
                    "success": False,
                    "error": str(e)
                }
        
        # ========================================
        # Pedidos Retidos Collections
        # ========================================
        retidos_collections = {
            "pedidos_retidos": COLLECTION_PEDIDOS_RETIDOS,
            "pedidos_retidos_chunks": COLLECTION_PEDIDOS_RETIDOS_CHUNKS,
            "pedidos_retidos_tabela": COLLECTION_PEDIDOS_RETIDOS_TABELA,
            "pedidos_retidos_tabela_chunks": COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS,
        }
        
        for name, collection_name in retidos_collections.items():
            try:
                previous_counts[name] = await db[collection_name].count_documents({})
                result = await db[collection_name].delete_many({})
                deleted_counts[name] = result.deleted_count
                results[name] = {
                    "success": True,
                    "deleted": result.deleted_count,
                    "previous": previous_counts[name]
                }
            except Exception as e:
                results[name] = {
                    "success": False,
                    "error": str(e)
                }
        
        # Calcular totais
        total_previous = sum(previous_counts.values())
        total_deleted = sum(deleted_counts.values())
        
        # Verificar se houve erros
        has_errors = any(not r.get("success", False) for r in results.values())
        
        return JSONResponse(
            status_code=200,
            content={
                "success": not has_errors,
                "message": "Limpeza de dados concluída",
                "warning": "⚠️ A coleção de TELEFONES foi preservada",
                "results": results,
                "summary": {
                    "total_previous": total_previous,
                    "total_deleted": total_deleted,
                    "collections_cleared": len([r for r in results.values() if r.get("success", False)]),
                    "collections_with_errors": len([r for r in results.values() if not r.get("success", False)])
                },
                "preserved_collections": [
                    "telefones"
                ]
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Erro ao limpar coleções: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao limpar coleções: {str(e)}"
        )

