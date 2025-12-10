"""
Rotas para mover remessas para devolução ou cobrar base
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import logging
from datetime import datetime
from app.services.database import get_database

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Sem Movimentação SC - Move"])

# Coleções para salvar as remessas movidas
COLLECTION_DEVOLUCAO = "sem_movimentacao_sc_devolucao"
COLLECTION_COBRAR_BASE = "sem_movimentacao_sc_cobrar_base"


class MoveRemessaRequest(BaseModel):
    remessa: str
    unidade_responsavel: str = None
    base_entrega: str = None
    tipo_ultima_operacao: str = None


@router.post("/move-to-devolucao")
async def move_to_devolucao(data: MoveRemessaRequest):
    """
    Move uma remessa para a coleção de devolução
    
    Salva os dados da remessa (Remessa, Unidade Responsável, Base de Entrega, Tipo da Última Operação)
    na coleção separada de devolução.
    """
    try:
        db = get_database()
        collection = db[COLLECTION_DEVOLUCAO]
        
        # Preparar documento para salvar
        documento = {
            "remessa": data.remessa,
            "unidade_responsavel": data.unidade_responsavel,
            "base_entrega": data.base_entrega,
            "tipo_ultima_operacao": data.tipo_ultima_operacao,
            "data_movimentacao": datetime.utcnow(),
            "tipo_movimentacao": "devolucao"
        }
        
        # Inserir na coleção de devolução
        result = await collection.insert_one(documento)
        
        if result.inserted_id:
            logger.info(f"Remessa {data.remessa} movida para devolução com sucesso")
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": f"Remessa {data.remessa} movida para devolução com sucesso",
                    "id": str(result.inserted_id)
                }
            )
        else:
            raise HTTPException(status_code=500, detail="Erro ao salvar remessa na coleção de devolução")
            
    except Exception as e:
        logger.error(f"Erro ao mover remessa para devolução: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao mover remessa para devolução: {str(e)}")


@router.post("/move-to-cobrar-base")
async def move_to_cobrar_base(data: MoveRemessaRequest):
    """
    Move uma remessa para a coleção de cobrar base
    
    Salva os dados da remessa (Remessa, Unidade Responsável, Base de Entrega, Tipo da Última Operação)
    na coleção separada de cobrar base.
    """
    try:
        db = get_database()
        collection = db[COLLECTION_COBRAR_BASE]
        
        # Preparar documento para salvar
        documento = {
            "remessa": data.remessa,
            "unidade_responsavel": data.unidade_responsavel,
            "base_entrega": data.base_entrega,
            "tipo_ultima_operacao": data.tipo_ultima_operacao,
            "data_movimentacao": datetime.utcnow(),
            "tipo_movimentacao": "cobrar_base"
        }
        
        # Inserir na coleção de cobrar base
        result = await collection.insert_one(documento)
        
        if result.inserted_id:
            logger.info(f"Remessa {data.remessa} movida para cobrar base com sucesso")
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": f"Remessa {data.remessa} movida para cobrar base com sucesso",
                    "id": str(result.inserted_id)
                }
            )
        else:
            raise HTTPException(status_code=500, detail="Erro ao salvar remessa na coleção de cobrar base")
            
    except Exception as e:
        logger.error(f"Erro ao mover remessa para cobrar base: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao mover remessa para cobrar base: {str(e)}")


@router.get("/devolucao/list")
async def listar_devolucao():
    """
    Lista todas as remessas na coleção de devolução
    """
    try:
        db = get_database()
        collection = db[COLLECTION_DEVOLUCAO]
        
        # Buscar todas as remessas
        cursor = collection.find({}).sort("data_movimentacao", -1)
        remessas = []
        async for document in cursor:
            remessas.append({
                "remessa": document.get("remessa"),
                "unidade_responsavel": document.get("unidade_responsavel"),
                "base_entrega": document.get("base_entrega"),
                "tipo_ultima_operacao": document.get("tipo_ultima_operacao"),
                "data_movimentacao": document.get("data_movimentacao").isoformat() if document.get("data_movimentacao") else None
            })
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": remessas,
                "total": len(remessas)
            }
        )
        
    except Exception as e:
        logger.error(f"Erro ao listar remessas em devolução: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao listar remessas em devolução: {str(e)}")

