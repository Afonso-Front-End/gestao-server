"""
Rota para verificar existência de dados
"""
from fastapi import APIRouter, HTTPException
import logging
from app.core.collections import COLLECTION_PEDIDOS_RETIDOS_CHUNKS
from app.services.database import db

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pedidos Retidos - Verificação"])

@router.get("/check-data")
async def check_has_data():
    """
    Verifica se existem dados de pedidos retidos no banco
    Usado para habilitar/desabilitar o upload de tabela de consultados
    
    Returns:
        {
            "success": true,
            "hasData": true/false,
            "count": número de documentos
        }
    """
    try:
        collection = db.database[COLLECTION_PEDIDOS_RETIDOS_CHUNKS]
        
        # Conta quantos documentos existem
        count = await collection.count_documents({})
        
        has_data = count > 0
        
        logger.info(f"✅ Verificação de dados - hasData={has_data}, count={count}")
        
        return {
            "success": True,
            "hasData": has_data,
            "count": count
        }
        
    except Exception as e:
        logger.error(f"❌ Erro ao verificar dados: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao verificar dados: {str(e)}")

