"""
Rotas de consulta de pedidos no galpão
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from bson.json_util import dumps
import json
from app.services.database import get_database
from app.core.collections import COLLECTION_SLA_PEDIDOS_GALPAO

router = APIRouter(tags=["Pedidos Galpão - Consulta"])

@router.get("/{base_name}")
async def get_pedidos_no_galpao(base_name: str) -> JSONResponse:
    """
    Busca pedidos que estão no galpão para uma base específica
    """
    try:
        db = get_database()
        
        # Buscar usando múltiplos formatos de base (para garantir que encontre)
        import re
        sigla_match = re.search(r'([A-Z]{2,4})', base_name.upper())
        sigla = sigla_match.group(1) if sigla_match else ""
        
        query_base = {
            "$or": [
                {"_base_name": base_name},
                {"_base_name": base_name.strip()},
                {"Base de entrega": base_name},
                {"Base de entrega": base_name.strip()},
                {"Base de escaneamento": base_name},
                {"Base de escaneamento": base_name.strip()},
            ]
        }
        
        # Adicionar busca por sigla se encontrada
        if sigla:
            query_base["$or"].extend([
                {"_base_name": {"$regex": sigla, "$options": "i"}},
                {"Base de entrega": {"$regex": sigla, "$options": "i"}},
                {"Base de escaneamento": {"$regex": sigla, "$options": "i"}},
            ])
        
        # Buscar pedidos no galpão para a base
        pedidos_raw = await db[COLLECTION_SLA_PEDIDOS_GALPAO].find(query_base).to_list(length=None)
        
        # Garantir que todos os pedidos tenham "Base de entrega" preenchido
        pedidos = []
        for pedido in pedidos_raw:
            # Garantir base de entrega
            if not pedido.get("Base de entrega") or pedido.get("Base de entrega") == "N/A":
                base_entrega = (
                    pedido.get("Base de Entrega") or
                    pedido.get("BASE") or
                    pedido.get("Unidade responsável") or
                    pedido.get("_base_name") or
                    base_name
                )
                pedido["Base de entrega"] = base_entrega
            pedidos.append(pedido)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "base_name": base_name,
                "total_pedidos": len(pedidos),
                "pedidos": pedidos
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao buscar pedidos no galpão: {str(e)}"
        )

@router.get("/{base_name}/motorista/{motorista}")
async def get_pedidos_no_galpao_por_motorista(base_name: str, motorista: str) -> JSONResponse:
    """
    Busca pedidos que estão no galpão para um motorista específico
    """
    try:
        db = get_database()
        
        # Buscar usando múltiplos formatos de base (para garantir que encontre)
        import re
        sigla_match = re.search(r'([A-Z]{2,4})', base_name.upper())
        sigla = sigla_match.group(1) if sigla_match else ""
        
        query_base = {
            "$or": [
                {"_base_name": base_name},
                {"_base_name": base_name.strip()},
                {"Base de entrega": base_name},
                {"Base de entrega": base_name.strip()},
                {"Base de escaneamento": base_name},
                {"Base de escaneamento": base_name.strip()},
            ]
        }
        
        # Adicionar busca por sigla se encontrada
        if sigla:
            query_base["$or"].extend([
                {"_base_name": {"$regex": sigla, "$options": "i"}},
                {"Base de entrega": {"$regex": sigla, "$options": "i"}},
                {"Base de escaneamento": {"$regex": sigla, "$options": "i"}},
            ])
        
        # Buscar pedidos no galpão para o motorista específico
        query_motorista = {
            "$and": [
                query_base,
                {
                    "$or": [
                        {"Responsável pela entrega": motorista},
                        {"Responsável pela entrega": motorista.strip()},
                        {"Responsavel pela entrega": motorista},
                        {"Responsavel pela entrega": motorista.strip()},
                        {"Entregador": motorista},
                        {"Entregador": motorista.strip()},
                        {"Motorista": motorista},
                        {"Motorista": motorista.strip()},
                    ]
                }
            ]
        }
        
        pedidos_raw = await db[COLLECTION_SLA_PEDIDOS_GALPAO].find(query_motorista).to_list(length=None)
        
        # Converter usando json_util para lidar com ObjectId
        pedidos_json = dumps(pedidos_raw)
        pedidos_list = json.loads(pedidos_json)
        
        # Garantir que todos os pedidos tenham "Base de entrega" preenchido
        pedidos = []
        for pedido in pedidos_list:
            # Garantir base de entrega
            if not pedido.get("Base de entrega") or pedido.get("Base de entrega") == "N/A":
                base_entrega = (
                    pedido.get("Base de Entrega") or
                    pedido.get("BASE") or
                    pedido.get("Unidade responsável") or
                    pedido.get("_base_name") or
                    base_name
                )
                pedido["Base de entrega"] = base_entrega
            pedidos.append(pedido)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "base_name": base_name,
                "motorista": motorista,
                "total_pedidos": len(pedidos),
                "pedidos": pedidos
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao buscar pedidos no galpão para o motorista: {str(e)}"
        )

@router.get("/")
async def get_all_pedidos_no_galpao() -> JSONResponse:
    """
    Busca todos os pedidos que estão no galpão
    """
    try:
        db = get_database()
        
        # Buscar todos os pedidos no galpão
        pedidos = await db[COLLECTION_SLA_PEDIDOS_GALPAO].find({}).to_list(length=None)
        
        # Agrupar por base
        bases = {}
        for pedido in pedidos:
            # Extrair base com fallbacks
            base_name = (
                pedido.get("Base de entrega") or 
                pedido.get("Base de Entrega") or
                pedido.get("BASE") or
                pedido.get("Unidade responsável") or
                pedido.get("_base_name") or
                "N/A"
            )
            # Garantir que não seja "N/A" ou vazio
            if not base_name or base_name == "N/A" or base_name.strip() == "":
                base_name = pedido.get("_base_name", "N/A")
            if base_name not in bases:
                bases[base_name] = []
            bases[base_name].append(pedido)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "total_pedidos": len(pedidos),
                "bases": bases
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao buscar pedidos no galpão: {str(e)}"
        )

