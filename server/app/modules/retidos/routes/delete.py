"""
Rotas de exclusão de dados de Pedidos Retidos
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.services.database import get_database
from app.core.collections import (
    COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS,
    COLLECTION_PEDIDOS_RETIDOS,
    COLLECTION_PEDIDOS_RETIDOS_CHUNKS,
    COLLECTION_PEDIDOS_RETIDOS_TABELA
)

router = APIRouter(tags=["Pedidos Retidos - Delete"])

@router.get("/tabela-chunks/check")
async def check_tabela_chunks_data() -> JSONResponse:
    """
    Verifica se existem dados na coleção pedidos_retidos_tabela_chunks
    Retorna informações sobre quantidade de documentos e última atualização
    """
    try:
        db = get_database()
        
        if db is None:
            raise HTTPException(status_code=500, detail="Não foi possível conectar ao banco de dados")
        
        # Contar documentos na coleção de chunks
        chunks_count = await db[COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS].count_documents({})
        
        # Buscar informação do documento principal mais recente
        tabela_collection = db[COLLECTION_PEDIDOS_RETIDOS_TABELA]
        latest_doc = await tabela_collection.find_one(
            {},
            sort=[("upload_date", -1)]
        )
        
        has_data = chunks_count > 0
        
        response_data = {
            "has_data": has_data,
            "chunks_count": chunks_count,
            "total_items": latest_doc.get("total_items", 0) if latest_doc else 0,
            "filename": latest_doc.get("filename", "") if latest_doc else "",
            "upload_date": latest_doc.get("upload_date").isoformat() if latest_doc and latest_doc.get("upload_date") else None
        }
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": response_data
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao verificar dados: {str(e)}"
        )

@router.delete("/tabela-chunks")
async def clear_pedidos_retidos_tabela_chunks() -> JSONResponse:
    """
    Limpa todos os dados da coleção pedidos_retidos_tabela_chunks
    """
    try:
        db = get_database()
        
        if db is None:
            raise HTTPException(status_code=500, detail="Não foi possível conectar ao banco de dados")
        
        # Contar antes da exclusão
        count_before = await db[COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS].count_documents({})
        
        # Deletar todos os documentos
        result = await db[COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS].delete_many({})
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Dados da coleção pedidos_retidos_tabela_chunks foram limpos com sucesso",
                "deleted_count": result.deleted_count,
                "previous_count": count_before
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao limpar dados: {str(e)}"
        )

@router.delete("/tabela")
async def clear_pedidos_retidos_tabela() -> JSONResponse:
    """
    Limpa todos os dados da coleção pedidos_retidos_tabela (dados do arquivo Consultados)
    """
    try:
        db = get_database()
        
        if db is None:
            raise HTTPException(status_code=500, detail="Não foi possível conectar ao banco de dados")
        
        # Contar antes da exclusão
        count_before = await db[COLLECTION_PEDIDOS_RETIDOS_TABELA].count_documents({})
        
        # Deletar todos os documentos
        result = await db[COLLECTION_PEDIDOS_RETIDOS_TABELA].delete_many({})
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Dados da coleção pedidos_retidos_tabela foram limpos com sucesso",
                "deleted_count": result.deleted_count,
                "previous_count": count_before
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao limpar dados: {str(e)}"
        )

@router.delete("/lotes")
async def clear_pedidos_lotes() -> JSONResponse:
    """
    Limpa todos os dados de lotes (pedidos filtrados/buscados)
    Deleta pedidos_retidos e pedidos_retidos_chunks (dados do primeiro upload)
    """
    try:
        db = get_database()
        
        if db is None:
            raise HTTPException(status_code=500, detail="Não foi possível conectar ao banco de dados")
        
        # Contar antes da exclusão
        count_pedidos_retidos = await db[COLLECTION_PEDIDOS_RETIDOS].count_documents({})
        count_pedidos_retidos_chunks = await db[COLLECTION_PEDIDOS_RETIDOS_CHUNKS].count_documents({})
        
        # Deletar todos os documentos das duas coleções
        result_pedidos_retidos = await db[COLLECTION_PEDIDOS_RETIDOS].delete_many({})
        result_pedidos_retidos_chunks = await db[COLLECTION_PEDIDOS_RETIDOS_CHUNKS].delete_many({})
        
        total_deleted = result_pedidos_retidos.deleted_count + result_pedidos_retidos_chunks.deleted_count
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Dados do arquivo 'Retidos' foram deletados com sucesso",
                "deleted_counts": {
                    "pedidos_retidos": result_pedidos_retidos.deleted_count,
                    "pedidos_retidos_chunks": result_pedidos_retidos_chunks.deleted_count,
                    "total": total_deleted
                },
                "previous_counts": {
                    "pedidos_retidos": count_pedidos_retidos,
                    "pedidos_retidos_chunks": count_pedidos_retidos_chunks
                }
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao deletar lotes: {str(e)}"
        )

@router.delete("/collections")
async def clear_pedidos_retidos_collections() -> JSONResponse:
    """
    Limpa todos os dados das coleções principais de Pedidos Retidos:
    - pedidos_retidos
    - pedidos_retidos_chunks
    - pedidos_retidos_tabela
    
    NOTA: Esta rota NÃO deleta pedidos_retidos_tabela_chunks (deletar separadamente se necessário)
    """
    try:
        db = get_database()
        
        if db is None:
            raise HTTPException(status_code=500, detail="Não foi possível conectar ao banco de dados")
        
        # Contar antes da exclusão
        count_pedidos_retidos = await db[COLLECTION_PEDIDOS_RETIDOS].count_documents({})
        count_pedidos_retidos_chunks = await db[COLLECTION_PEDIDOS_RETIDOS_CHUNKS].count_documents({})
        count_pedidos_retidos_tabela = await db[COLLECTION_PEDIDOS_RETIDOS_TABELA].count_documents({})
        
        # Deletar todos os documentos das três coleções
        result_pedidos_retidos = await db[COLLECTION_PEDIDOS_RETIDOS].delete_many({})
        result_pedidos_retidos_chunks = await db[COLLECTION_PEDIDOS_RETIDOS_CHUNKS].delete_many({})
        result_pedidos_retidos_tabela = await db[COLLECTION_PEDIDOS_RETIDOS_TABELA].delete_many({})
        
        total_deleted = (
            result_pedidos_retidos.deleted_count +
            result_pedidos_retidos_chunks.deleted_count +
            result_pedidos_retidos_tabela.deleted_count
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Dados das coleções principais de Pedidos Retidos foram limpos com sucesso",
                "deleted_counts": {
                    "pedidos_retidos": result_pedidos_retidos.deleted_count,
                    "pedidos_retidos_chunks": result_pedidos_retidos_chunks.deleted_count,
                    "pedidos_retidos_tabela": result_pedidos_retidos_tabela.deleted_count,
                    "total": total_deleted
                },
                "previous_counts": {
                    "pedidos_retidos": count_pedidos_retidos,
                    "pedidos_retidos_chunks": count_pedidos_retidos_chunks,
                    "pedidos_retidos_tabela": count_pedidos_retidos_tabela
                },
                "warning": "⚠️ A coleção pedidos_retidos_tabela_chunks não foi deletada"
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao limpar dados: {str(e)}"
        )

