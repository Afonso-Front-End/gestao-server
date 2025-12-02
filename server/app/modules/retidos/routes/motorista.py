"""
Rota para buscar pedidos por motorista e gerenciar status
"""
from fastapi import APIRouter, HTTPException, Body, Path, Query
from pydantic import BaseModel, Field
from typing import Optional, Literal
import logging
from app.core.collections import (
    COLLECTION_PEDIDOS_RETIDOS_CHUNKS,
    COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS
)
from app.services.database import get_database
from .helpers import (
    get_numero_pedido,
    get_base_entrega,
    get_responsavel,
    get_marca_assinatura,
    is_child_pedido,
    is_entregue,
    is_nao_entregue,
    normalize_string
)

class StatusMotoristaModel(BaseModel):
    status: Optional[str] = None
    responsavel: str
    base: Optional[str] = None
    observacao: Optional[str] = Field(None, max_length=500, description="Observação sobre o status")

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pedidos Retidos - Motorista"])

@router.get("/pedidos-motorista/{motorista}")
async def get_pedidos_motorista(
    motorista: str = Path(..., min_length=1, description="Nome do motorista"),
    base: str | None = Query(None, description="Base para filtrar"),
    status: Literal["nao_entregues", "entregues"] | None = Query(None, description="Filtrar por status: 'nao_entregues' ou 'entregues'"),
):
    """
    Busca pedidos de um motorista específico com merge de dados de tabela_dados_chunks e pedidos_retidos_chunks
    """
    try:
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
            
        collection = db[COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS]
        total = await collection.count_documents({})
        if total == 0:
            return {"success": True, "data": [], "total_pedidos": 0}

        # Pré-carregar mapa com dados de pedidos_retidos_chunks para merge/enriquecimento
        collection_retidos = db[COLLECTION_PEDIDOS_RETIDOS_CHUNKS]
        mapa_retidos: dict[str, dict] = {}
        total_retidos = await collection_retidos.count_documents({})
        if total_retidos > 0:
            cur_r = collection_retidos.find({}).sort("chunk_number", 1)
            async for ch in cur_r:
                for ped in ch.get("chunk_data", []) or []:
                    # Chave de união: usar número do pedido
                    chave = get_numero_pedido(ped)
                    if chave:
                        mapa_retidos[chave] = ped

        itens: list[dict] = []
        cursor = collection.find({}).sort("chunk_number", 1)
        motorista_norm = normalize_string(motorista)
        base_norm = normalize_string(base) if base else None

        async for chunk in cursor:
            for item in chunk.get("data", []) or []:
                # Verificar se é o motorista correto
                responsavel = get_responsavel(item)
                if normalize_string(responsavel) != motorista_norm:
                    continue
                
                # Filtrar por base se fornecido
                base_item = get_base_entrega(item)
                if base_norm and normalize_string(base_item) != base_norm:
                    continue
                
                # Filtrar por status se fornecido
                marca = get_marca_assinatura(item)
                numero = get_numero_pedido(item)
                
                if status == "nao_entregues" and not is_nao_entregue(marca):
                    # Tentar complementar com dados dos retidos
                    pr_try = mapa_retidos.get(numero) if numero else None
                    marca2 = get_marca_assinatura(pr_try) if pr_try else ""
                    if not is_nao_entregue(marca2):
                        continue
                
                if status == "entregues" and not is_entregue(marca):
                    # Tentar complementar com dados dos retidos
                    pr_try = mapa_retidos.get(numero) if numero else None
                    marca2 = get_marca_assinatura(pr_try) if pr_try else ""
                    if not is_entregue(marca2):
                        continue
                
                # Remover pedidos filhos e vazios
                if not numero or is_child_pedido(numero):
                    continue
                
                # Montar base a partir de tabela_dados_chunks
                remessa = numero
                enriched = {
                    "Remessa": remessa,
                    "Número de pedido JMS": remessa,  # Garantir que este campo também esteja presente
                    "Unidade responsável": item.get("Base de entrega", "") or item.get("BASE", ""),
                    "Cidade Destino": item.get("Cidade Destino", "") or item.get("Cidade", ""),
                    "Destinatário": item.get("Destinatário", "") or item.get("DESTINATÁRIO", ""),
                    "CEP destino": item.get("CEP destino", "") or item.get("CEP", ""),
                    "Marca de assinatura": item.get("Marca de assinatura", "") or item.get("Status", "") or item.get("Situacao", ""),
                    "Base de entrega": item.get("Base de entrega", "") or item.get("BASE", ""),
                    # Complemento (várias possíveis chaves)
                    "Complemento": (
                        item.get("Complemento")
                        or item.get("Complemento do Endereço")
                        or item.get("Complemento do endereco")
                        or item.get("COMPLEMENTO")
                        or item.get("Compl.")
                        or item.get("Compl")
                        or item.get("Complemento Endereço")
                        or item.get("Complemento endereco")
                        or item.get("Complemento End.")
                        or item.get("COMPLEMENTO ENDERECO")
                        or ""
                    ),
                }

                # Merge com dados de pedidos_retidos_chunks (se existir chave)
                if remessa and remessa in mapa_retidos:
                    pr = mapa_retidos[remessa]
                    # Usar número dos retidos se disponível
                    numero_retidos = get_numero_pedido(pr) or remessa
                    enriched.update({
                        "Remessa": numero_retidos,  # Atualizar com número dos retidos se disponível
                        "Número de pedido JMS": numero_retidos,  # Garantir consistência
                        "Tipo da última operação": pr.get("Tipo da última operação", ""),
                        "Operador do bipe mais recente": pr.get("Operador do bipe mais recente", ""),
                        "Horário da última operação": pr.get("Horário da última operação", ""),
                        "Aging": pr.get("Aging", ""),
                        "Regional mais recente": pr.get("Regional mais recente", ""),
                        # manter Base de entrega se vier mais atualizada
                        "Base de entrega": pr.get("Base de entrega", enriched["Base de entrega"]),
                        # Complemento também do retidos se presente
                        "Complemento": (
                            pr.get("Complemento")
                            or pr.get("Complemento do Endereço")
                            or pr.get("Complemento do endereco")
                            or pr.get("COMPLEMENTO")
                            or pr.get("Compl.")
                            or pr.get("Compl")
                            or pr.get("Complemento Endereço")
                            or pr.get("Complemento endereco")
                            or pr.get("Complemento End.")
                            or pr.get("COMPLEMENTO ENDERECO")
                            or enriched.get("Complemento", "")
                        ),
                    })

                itens.append(enriched)

        return {"success": True, "data": itens, "total_pedidos": len(itens)}
    except Exception as e:
        logger.error(f"Erro em pedidos-motorista: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.post("/motorista/{motorista}/status")
async def salvar_status_motorista(
    motorista: str,
    status_data: StatusMotoristaModel = Body(...)
):
    """
    Salva o status de um motorista (Retornou, Não retornou, Esperando retorno, Número de contato errado)
    """
    try:
        from datetime import datetime
        
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        # Usar uma coleção específica para status dos motoristas
        collection_name = "motoristas_status"
        collection = db[collection_name]
        
        status_value = status_data.status  # Pode ser 'ok', 'no', 'pendente', 'sem_telefone' ou null
        responsavel = status_data.responsavel or motorista
        base = status_data.base or ""
        observacao = status_data.observacao or ""
        
        # Buscar status existente usando chave composta (responsavel + base)
        if base:
            query = {"responsavel": responsavel, "base": base}
        else:
            # Se não tiver base, buscar apenas por responsavel sem base ou com base vazia (para compatibilidade com dados antigos)
            query = {
                "responsavel": responsavel,
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
                "message": f"Status removido para {responsavel}",
                "status": None
            }
        else:
            # Validar status - valores permitidos (atualizados para corresponder ao frontend)
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
            
            # Atualizar ou criar documento com chave composta (responsavel + base)
            doc = {
                "responsavel": responsavel,
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
                "message": f"Status {result_status} com sucesso para {responsavel}",
                "status": status_value,
                "responsavel": responsavel
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao salvar status do motorista: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/motorista/all-status")
async def obter_todos_status():
    """
    Obtém todos os status salvos (para carregar observações ao iniciar)
    """
    try:
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        collection_name = "motoristas_status"
        collection = db[collection_name]
        
        # Buscar todos os status
        cursor = collection.find({})
        statuses = []
        
        async for doc in cursor:
            # Remover _id do MongoDB para serialização
            doc.pop('_id', None)
            statuses.append(doc)
        
        return {
            "success": True,
            "statuses": statuses,
            "total": len(statuses)
        }
        
    except Exception as e:
        logger.error(f"Erro ao obter todos os status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/motorista/{motorista}/status")
async def obter_status_motorista(motorista: str, base: str | None = None):
    """
    Obtém o status de um motorista usando chave composta (responsavel + base)
    """
    try:
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        collection_name = "motoristas_status"
        collection = db[collection_name]
        
        # Buscar usando chave composta (responsavel + base)
        if base:
            query = {"responsavel": motorista, "base": base}
        else:
            # Se não tiver base, buscar apenas por responsavel sem base ou com base vazia (para compatibilidade com dados antigos)
            query = {
                "responsavel": motorista,
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
                "responsavel": doc.get("responsavel"),
                "base": doc.get("base"),
                "updated_at": doc.get("updated_at")
            }
        else:
            return {
                "success": True,
                "status": None,
                "responsavel": motorista,
                "base": base,
                "message": "Nenhum status encontrado"
            }
            
    except Exception as e:
        logger.error(f"Erro ao obter status do motorista: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

