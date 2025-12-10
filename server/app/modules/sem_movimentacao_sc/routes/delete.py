"""
Rotas para limpar dados de Sem Movimentação SC
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
import logging
from datetime import datetime
from app.services.database import get_database
from app.core.collections import COLLECTION_SEM_MOVIMENTACAO_SC, COLLECTION_SEM_MOVIMENTACAO_SC_CHUNKS

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Sem Movimentação SC - Delete"])


@router.delete("/clear")
async def clear_sem_movimentacao_sc():
    """
    Limpa todos os dados de Sem Movimentação SC do banco de dados.
    
    Remove todos os documentos das coleções:
    - sem_movimentacao_sc (metadados dos arquivos)
    - sem_movimentacao_sc_chunks (chunks dos dados)
    
    Retorna estatísticas sobre os dados removidos.
    """
    try:
        db = get_database()
        
        # Contar documentos antes de deletar
        main_collection = db[COLLECTION_SEM_MOVIMENTACAO_SC]
        chunks_collection = db[COLLECTION_SEM_MOVIMENTACAO_SC_CHUNKS]
        
        count_main_before = await main_collection.count_documents({})
        count_chunks_before = await chunks_collection.count_documents({})
        
        logger.info(f"🗑️ Iniciando limpeza de dados de Sem Movimentação SC")
        logger.info(f"   Documentos principais: {count_main_before}")
        logger.info(f"   Chunks: {count_chunks_before}")
        
        # Deletar todos os documentos das coleções
        result_main = await main_collection.delete_many({})
        result_chunks = await chunks_collection.delete_many({})
        
        deleted_main = result_main.deleted_count
        deleted_chunks = result_chunks.deleted_count
        
        logger.info(f"✅ Limpeza concluída:")
        logger.info(f"   Documentos principais removidos: {deleted_main}")
        logger.info(f"   Chunks removidos: {deleted_chunks}")
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Dados limpos com sucesso",
                "deleted": {
                    "main_documents": deleted_main,
                    "chunks": deleted_chunks,
                    "total": deleted_main + deleted_chunks
                },
                "cleared_at": datetime.now().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Erro ao limpar dados de Sem Movimentação SC: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

