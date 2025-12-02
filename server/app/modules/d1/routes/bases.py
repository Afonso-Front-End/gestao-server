"""
Rotas para buscar bases únicas dos dados D-1
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.services.database import get_database
from app.core.collections import COLLECTION_D1_CHUNKS
import logging

logger = logging.getLogger(__name__)

router = APIRouter(tags=["D-1 - Bases"])

@router.get("/bases")
async def get_d1_bases():
    """
    Retorna todas as bases únicas encontradas na coluna "Base de entrega" dos dados D-1
    """
    try:
        db = get_database()
        collection = db[COLLECTION_D1_CHUNKS]
        
        # Usar aggregation para buscar bases únicas de todos os chunks
        pipeline = [
            # Desempacotar todos os chunks
            {"$unwind": "$chunk_data"},
            # Extrair apenas a coluna "Base de entrega"
            {"$project": {
                "base_entrega": "$chunk_data.Base de entrega"
            }},
            # Filtrar apenas valores não-nulos e não-vazios
            {"$match": {
                "base_entrega": {"$exists": True, "$ne": None, "$ne": ""}
            }},
            # Agrupar por base e pegar valores únicos
            {"$group": {
                "_id": "$base_entrega"
            }},
            # Ordenar alfabeticamente
            {"$sort": {"_id": 1}},
            # Projetar apenas o nome da base
            {"$project": {
                "_id": 0,
                "base": "$_id"
            }}
        ]
        
        bases = []
        async for doc in collection.aggregate(pipeline):
            base = doc.get('base', '').strip()
            if base:
                bases.append(base)
        
        logger.info(f"📊 Encontradas {len(bases)} bases únicas nos dados D-1")
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": bases,
                "total": len(bases)
            }
        )
    except Exception as e:
        logger.error(f"Erro ao buscar bases D-1: {e}")
        raise HTTPException(status_code=500, detail=str(e))


