"""
Rotas para gerenciar status de motoristas na SLA
"""
from fastapi import APIRouter, HTTPException, Body, Path, Query
from pydantic import BaseModel, Field
from typing import Optional
import logging
from app.services.database import get_database

class StatusMotoristaSLAModel(BaseModel):
    status: Optional[str] = None
    motorista: str
    base: Optional[str] = None
    observacao: Optional[str] = Field(None, max_length=500, description="Observação sobre o status")

logger = logging.getLogger(__name__)

router = APIRouter(tags=["SLA - Motorista Status"])

@router.post("/motorista/{motorista}/status")
async def salvar_status_motorista_sla(
    motorista: str,
    status_data: StatusMotoristaSLAModel = Body(...)
):
    """
    Salva o status de um motorista na SLA (Retornou, Não retornou, Esperando retorno, Número de contato errado)
    """
    try:
        from datetime import datetime
        
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        # Usar coleção específica para status dos motoristas SLA
        collection_name = "motorista_status_sla"
        collection = db[collection_name]
        
        status_value = status_data.status  # Pode ser 'Retornou', 'Não retornou', 'Esperando retorno', 'Número de contato errado' ou null
        motorista_value = status_data.motorista or motorista
        base = status_data.base or ""
        observacao = status_data.observacao or ""
        
        # Buscar status existente usando chave composta (motorista + base)
        if base:
            query = {"motorista": motorista_value, "base": base}
        else:
            # Se não tiver base, buscar apenas por motorista sem base ou com base vazia
            query = {
                "motorista": motorista_value,
                "$or": [
                    {"base": {"$exists": False}},
                    {"base": None},
                    {"base": ""}
                ]
            }
        
        existing = await collection.find_one(query)
        
        if status_value is None:
            # Se status for null, remover o documento
            if existing:
                await collection.delete_one({"_id": existing["_id"]})
            return {
                "success": True,
                "message": f"Status removido para {motorista_value}",
                "status": None
            }
        else:
            # Validar status - valores permitidos
            STATUS_VALIDOS = [
                'Retornou',
                'Não retornou',
                'Esperando retorno',
                'Número de contato errado'
            ]
            if status_value not in STATUS_VALIDOS:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Status inválido: {status_value}. Valores permitidos: {', '.join(STATUS_VALIDOS)}"
                )
            
            # Atualizar ou criar documento com chave composta (motorista + base)
            doc = {
                "motorista": motorista_value,
                "base": base,
                "status": status_value,
                "observacao": observacao,
                "updated_at": datetime.now()
            }
            
            if existing:
                # Atualizar existente
                await collection.update_one(
                    {"_id": existing["_id"]},
                    {"$set": doc}
                )
                result_status = "atualizado"
            else:
                # Criar novo
                doc["created_at"] = datetime.now()
                await collection.insert_one(doc)
                result_status = "criado"
            
            return {
                "success": True,
                "message": f"Status {result_status} com sucesso para {motorista_value}",
                "status": status_value,
                "motorista": motorista_value
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao salvar status do motorista SLA: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/motorista/all-status")
async def obter_todos_status_sla():
    """
    Obtém todos os status de motoristas salvos na SLA
    """
    try:
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        collection_name = "motorista_status_sla"
        collection = db[collection_name]
        
        # Buscar todos os status
        cursor = collection.find({})
        statuses = await cursor.to_list(length=None)
        
        # Formatar resposta
        formatted_statuses = []
        for doc in statuses:
            formatted_statuses.append({
                "motorista": doc.get("motorista"),
                "base": doc.get("base", ""),
                "status": doc.get("status"),
                "observacao": doc.get("observacao", ""),
                "created_at": doc.get("created_at"),
                "updated_at": doc.get("updated_at")
            })
        
        return {
            "success": True,
            "statuses": formatted_statuses,
            "total": len(formatted_statuses)
        }
            
    except Exception as e:
        logger.error(f"Erro ao obter todos os status SLA: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/motorista/{motorista}/status")
async def obter_status_motorista_sla(motorista: str, base: Optional[str] = Query(None)):
    """
    Obtém o status de um motorista usando chave composta (motorista + base)
    """
    try:
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        collection_name = "motorista_status_sla"
        collection = db[collection_name]
        
        # Buscar usando chave composta (motorista + base)
        if base:
            query = {"motorista": motorista, "base": base}
        else:
            # Se não tiver base, buscar apenas por motorista sem base ou com base vazia
            query = {
                "motorista": motorista,
                "$or": [
                    {"base": {"$exists": False}},
                    {"base": None},
                    {"base": ""}
                ]
            }
        
        doc = await collection.find_one(query)
        
        if doc:
            return {
                "success": True,
                "status": doc.get("status"),
                "motorista": doc.get("motorista"),
                "base": doc.get("base", ""),
                "observacao": doc.get("observacao", ""),
                "updated_at": doc.get("updated_at")
            }
        else:
            return {
                "success": True,
                "status": None,
                "motorista": motorista,
                "base": base or "",
                "message": "Nenhum status encontrado"
            }
            
    except Exception as e:
        logger.error(f"Erro ao obter status do motorista SLA: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

