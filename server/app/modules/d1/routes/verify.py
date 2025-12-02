"""
Rotas para verificar e consultar dados D-1
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.services.database import get_database
from app.core.collections import COLLECTION_D1_MAIN, COLLECTION_D1_CHUNKS
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)

router = APIRouter(tags=["D-1 - Verify"])

@router.get("/verify-chunks/{main_document_id}")
async def verify_chunks(main_document_id: str):
    """
    Verifica se os chunks foram salvos corretamente para um documento principal
    """
    try:
        db = get_database()
        
        # Verificar documento principal
        main_collection = db[COLLECTION_D1_MAIN]
        main_doc = await main_collection.find_one({"_id": ObjectId(main_document_id)})
        
        if not main_doc:
            raise HTTPException(status_code=404, detail="Documento principal não encontrado")
        
        # Verificar chunks
        chunks_collection = db[COLLECTION_D1_CHUNKS]
        
        # Contar chunks salvos
        total_chunks_saved = await chunks_collection.count_documents({
            "main_document_id": main_document_id
        })
        
        # Buscar informações dos chunks
        chunks_info = []
        total_records_in_chunks = 0
        
        async for chunk in chunks_collection.find({
            "main_document_id": main_document_id
        }).sort("chunk_number", 1):
            chunk_size = chunk.get('chunk_size', 0)
            total_records_in_chunks += chunk_size
            chunks_info.append({
                "chunk_number": chunk.get('chunk_number'),
                "chunk_size": chunk_size,
                "has_data": 'chunk_data' in chunk and len(chunk.get('chunk_data', [])) > 0,
                "data_count": len(chunk.get('chunk_data', [])) if 'chunk_data' in chunk else 0
            })
        
        # Comparar com o esperado
        expected_chunks = main_doc.get('total_chunks', 0)
        expected_items = main_doc.get('total_items', 0)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "main_document": {
                    "id": main_document_id,
                    "filename": main_doc.get('filename'),
                    "expected_chunks": expected_chunks,
                    "expected_items": expected_items,
                    "status": main_doc.get('status')
                },
                "chunks_verification": {
                    "total_chunks_saved": total_chunks_saved,
                    "expected_chunks": expected_chunks,
                    "chunks_match": total_chunks_saved == expected_chunks,
                    "total_records_in_chunks": total_records_in_chunks,
                    "expected_items": expected_items,
                    "records_match": total_records_in_chunks == expected_items,
                    "chunks_info": chunks_info[:20]  # Primeiros 20 chunks
                },
                "summary": {
                    "all_chunks_saved": total_chunks_saved == expected_chunks,
                    "all_records_saved": total_records_in_chunks == expected_items,
                    "status": "✅ OK" if (total_chunks_saved == expected_chunks and total_records_in_chunks == expected_items) else "⚠️ VERIFICAR"
                }
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao verificar chunks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sample-chunk/{main_document_id}/{chunk_number}")
async def get_sample_chunk(main_document_id: str, chunk_number: int, sample_size: int = 5):
    """
    Retorna uma amostra dos dados de um chunk específico
    
    Args:
        main_document_id: ID do documento principal
        chunk_number: Número do chunk
        sample_size: Quantidade de registros para retornar (padrão: 5)
    """
    try:
        db = get_database()
        collection = db[COLLECTION_D1_CHUNKS]
        
        chunk = await collection.find_one({
            "main_document_id": main_document_id,
            "chunk_number": chunk_number
        })
        
        if not chunk:
            raise HTTPException(status_code=404, detail=f"Chunk {chunk_number} não encontrado")
        
        chunk_data = chunk.get('chunk_data', [])
        
        # Retornar apenas uma amostra
        sample_data = chunk_data[:sample_size] if len(chunk_data) > sample_size else chunk_data
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "chunk_info": {
                    "chunk_number": chunk.get('chunk_number'),
                    "chunk_size": chunk.get('chunk_size'),
                    "total_records": len(chunk_data),
                    "sample_size": len(sample_data)
                },
                "sample_data": sample_data,
                "columns": list(sample_data[0].keys()) if sample_data else []
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar amostra do chunk: {e}")
        raise HTTPException(status_code=500, detail=str(e))


