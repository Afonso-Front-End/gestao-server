"""
Rota para salvar lista de telefones em chunks
"""
from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
from datetime import datetime
import logging
import re
from app.core.collections import COLLECTION_TELEFONES
from app.services.database import db

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Lista de Telefones - Chunks"])

@router.post("/salvar-chunk")
async def salvar_lista_em_chunks(items: List[Dict[str, Any]]):
    """
    Recebe uma lista de itens e salva em chunks na coleção de telefones.
    Cada item é enriquecido com timestamp e origem 'bulk_chunk'.
    """
    try:
        if not isinstance(items, list) or not items:
            raise HTTPException(status_code=400, detail="Lista de itens inválida ou vazia")

        collection = db.database[COLLECTION_TELEFONES]

        ts = datetime.now().isoformat()
        docs = []
        for raw in items:
            item = dict(raw)
            # Normalizar HUB se presente
            if 'HUB' in item and item['HUB']:
                hub_upper = str(item['HUB']).strip().upper()
                m = re.search(r'([A-Z]{2,4})', hub_upper)
                sigla = m.group(1) if m else ''
                item['HUB'] = f"{sigla} -SC" if sigla else hub_upper
            item['timestamp'] = ts
            item['origem'] = item.get('origem', 'bulk_chunk')
            docs.append(item)

        # Inserir em chunks de 1000
        chunk_size = 1000
        total = 0
        for i in range(0, len(docs), chunk_size):
            chunk = docs[i:i+chunk_size]
            if chunk:
                result = await collection.insert_many(chunk)
                total += len(result.inserted_ids)

        return { 'success': True, 'inserted': total }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao salvar em chunks: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {e}")

