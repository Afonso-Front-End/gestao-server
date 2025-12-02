"""
Rotas para listar dados D-1
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from app.services.database import get_database
from app.core.collections import COLLECTION_D1_MAIN, COLLECTION_D1_CHUNKS
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)

router = APIRouter(tags=["D-1 - List"])

@router.get("/main-documents")
async def list_main_documents():
    """
    Lista todos os documentos principais D-1
    """
    try:
        db = get_database()
        collection = db[COLLECTION_D1_MAIN]
        
        documents = []
        async for doc in collection.find({}).sort("upload_date", -1):
            doc['_id'] = str(doc['_id'])
            documents.append(doc)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": documents,
                "total": len(documents)
            }
        )
    except Exception as e:
        logger.error(f"Erro ao listar documentos principais: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/chunks/{main_document_id}")
async def list_chunks(
    main_document_id: str,
    chunk_number: int = Query(None, description="Número do chunk específico"),
    limit: int = Query(10, description="Limite de chunks a retornar")
):
    """
    Lista chunks de um documento principal D-1
    
    Args:
        main_document_id: ID do documento principal
        chunk_number: (opcional) Número do chunk específico
        limit: Limite de chunks a retornar
    """
    try:
        db = get_database()
        collection = db[COLLECTION_D1_CHUNKS]
        
        # Construir query
        query = {"main_document_id": main_document_id}
        if chunk_number:
            query["chunk_number"] = chunk_number
        
        # Buscar chunks
        chunks = []
        cursor = collection.find(query).sort("chunk_number", 1).limit(limit)
        
        async for chunk in cursor:
            chunk['_id'] = str(chunk['_id'])
            # Não retornar os dados completos para não sobrecarregar
            if 'chunk_data' in chunk:
                chunk['chunk_data_count'] = len(chunk['chunk_data'])
                # Remover dados completos (muito grande)
                del chunk['chunk_data']
            chunks.append(chunk)
        
        # Contar total de chunks
        total_chunks = await collection.count_documents({"main_document_id": main_document_id})
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "main_document_id": main_document_id,
                "data": chunks,
                "total_chunks": total_chunks,
                "returned": len(chunks)
            }
        )
    except Exception as e:
        logger.error(f"Erro ao listar chunks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/chunk-data/{main_document_id}/{chunk_number}")
async def get_chunk_data(main_document_id: str, chunk_number: int):
    """
    Retorna os dados completos de um chunk específico
    
    Args:
        main_document_id: ID do documento principal
        chunk_number: Número do chunk
    """
    try:
        db = get_database()
        collection = db[COLLECTION_D1_CHUNKS]
        
        chunk = await collection.find_one({
            "main_document_id": main_document_id,
            "chunk_number": chunk_number
        })
        
        if not chunk:
            raise HTTPException(status_code=404, detail="Chunk não encontrado")
        
        chunk['_id'] = str(chunk['_id'])
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": chunk
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar chunk: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats/{main_document_id}")
async def get_stats(main_document_id: str):
    """
    Retorna estatísticas de um documento principal D-1
    """
    try:
        db = get_database()
        
        # Buscar documento principal
        main_collection = db[COLLECTION_D1_MAIN]
        main_doc = await main_collection.find_one({"_id": ObjectId(main_document_id)})
        
        if not main_doc:
            raise HTTPException(status_code=404, detail="Documento principal não encontrado")
        
        # Buscar estatísticas dos chunks
        chunks_collection = db[COLLECTION_D1_CHUNKS]
        total_chunks = await chunks_collection.count_documents({"main_document_id": main_document_id})
        
        # Calcular tamanho total dos chunks (aproximado)
        chunks = []
        total_records = 0
        async for chunk in chunks_collection.find({"main_document_id": main_document_id}):
            chunk_size = chunk.get('chunk_size', 0)
            total_records += chunk_size
            chunks.append({
                "chunk_number": chunk.get('chunk_number'),
                "chunk_size": chunk_size
            })
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "main_document": {
                    "_id": str(main_doc['_id']),
                    "filename": main_doc.get('filename'),
                    "upload_date": str(main_doc.get('upload_date')),
                    "total_items": main_doc.get('total_items'),
                    "total_chunks": main_doc.get('total_chunks'),
                    "status": main_doc.get('status'),
                    "processing_time": main_doc.get('processing_time'),
                    "columns_found": main_doc.get('columns_found', [])
                },
                "chunks_stats": {
                    "total_chunks": total_chunks,
                    "total_records": total_records,
                    "chunks": chunks[:10]  # Primeiros 10 chunks
                }
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar estatísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


