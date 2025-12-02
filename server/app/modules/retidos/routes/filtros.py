"""
Rotas para buscar pedidos filtrados e pedidos parados
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import Field
import logging
import re
from app.core.collections import (
    COLLECTION_PEDIDOS_RETIDOS_CHUNKS,
    COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS
)
from app.services.database import db
from .helpers import (
    matches_filters,
    extract_pedido_columns,
    get_numero_pedido,
    get_base_entrega,
    get_cidade_destino,
    get_responsavel,
    get_marca_assinatura,
    is_child_pedido,
    is_entregue,
    is_nao_entregue,
    extract_raiz_numero
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pedidos Retidos - Filtros"])

@router.get("/filtered-pedidos")
async def get_filtered_pedidos(
    bases: str | None = Query(None, description="Bases separadas por vírgula"),
    tipos: str | None = Query(None, description="Tipos de operação separados por vírgula"),
    aging: str | None = Query(None, description="Aging separados por vírgula"),
    limit: int = Query(default=1000, ge=1, le=10000, description="Limite de resultados (1-10000)"),
):
    """
    🔍 BUSCAR PEDIDOS FILTRADOS (para criação de lotes)
    Fonte: pedidos_retidos_chunks (campo chunk_data)
    Filtros: bases (Unidade responsável/BASE), tipos, aging
    """
    try:
        # Normalizar filtros
        bases_list = [b.strip() for b in bases.split(',')] if bases else []
        tipos_list = [t.strip() for t in tipos.split(',')] if tipos else []
        aging_list = [a.strip() for a in aging.split(',')] if aging else []

        logger.info(
            f"🔍 Filtros applied - Bases: {bases_list}, Tipos: {tipos_list}, Aging: {aging_list}, limit={limit}"
        )

        # Ler diretamente dos chunks de pedidos retidos
        collection = db.database[COLLECTION_PEDIDOS_RETIDOS_CHUNKS]
        total_chunks = await collection.count_documents({})
        if total_chunks == 0:
            return {"data": [], "total_found": 0, "total_processed": 0}

        cursor = collection.find({}).sort("chunk_number", 1)
        pedidos_filtrados: list[dict] = []
        numeros_vistos: set[str] = set()
        raizes_vistas: set[str] = set()
        total_processados = 0

        async for chunk in cursor:
            dados_chunk = chunk.get("chunk_data", []) or []
            for item in dados_chunk:
                total_processados += 1
                
                # Aplicar filtros
                if not matches_filters(item, bases_list, tipos_list, aging_list):
                    continue
                
                # Extrair e validar número do pedido
                numero_str = get_numero_pedido(item)
                if not numero_str:
                    continue
                
                # Remover pedidos filhos
                if is_child_pedido(numero_str):
                    continue
                
                # Deduplicar por número bruto
                if numero_str in numeros_vistos:
                    continue
                numeros_vistos.add(numero_str)

                # Deduplicar por raiz numérica
                raiz_numerica = extract_raiz_numero(numero_str)
                if raiz_numerica:
                    if raiz_numerica in raizes_vistas:
                        continue
                    raizes_vistas.add(raiz_numerica)

                # Extrair colunas do pedido
                pedido = extract_pedido_columns(item)
                if pedido:
                    pedidos_filtrados.append(pedido)
                    if limit > 0 and len(pedidos_filtrados) >= limit:
                        break
            if limit > 0 and len(pedidos_filtrados) >= limit:
                break
        
        logger.info(
            f"📊 filtered-pedidos → processados={total_processados}, encontrados={len(pedidos_filtrados)}"
        )

        return {
            "data": pedidos_filtrados,
            "total_found": len(pedidos_filtrados),
            "total_processed": total_processados,
            "filters_applied": {
                "bases": bases_list,
                "tipos": tipos_list,
                "aging": aging_list,
            },
        }
    except Exception as e:
        logger.error(f"Erro em filtered-pedidos: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/pedidos-parados")
async def get_pedidos_parados(
    bases: str | None = Query(None, description="Bases separadas por vírgula"),
    tipos: str | None = Query(None, description="Tipos de operação separados por vírgula"),
    aging: str | None = Query(None, description="Aging separados por vírgula"),
    cidades: str | None = Query(None, description="Cidades separadas por vírgula"),
):
    """
    📊 CONTROLE DE PEDIDOS PARADOS (agrupado por responsável)
    - Fonte: pedidos_retidos_chunks (chunk_data)
    - Aplica filtros (bases/tipos/aging/cidades) reutilizando _matches_filters
    - Remove pedidos filhos e vazios
    - Dedup por raiz numérica
    - Conta total/entregues/não_entregues por responsável
    """
    try:
        # Normalizar filtros
        bases_list = [b.strip() for b in bases.split(',')] if bases else []
        tipos_list = [t.strip() for t in tipos.split(',')] if tipos else []
        aging_list = [a.strip() for a in aging.split(',')] if aging else []
        cidades_list = [c.strip().upper() for c in cidades.split(',')] if cidades else []

        # Fonte requerida: tabela_dados_chunks (campo 'data')
        collection = db.database[COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS]
        total_chunks = await collection.count_documents({})
        if total_chunks == 0:
            return {
                "success": True,
                "data": [],
                "total_responsaveis": 0,
                "total_pedidos": 0,
            }

        raiz_vistas: set[str] = set()
        stats: dict[str, dict] = {}
        total_validos = 0

        cursor = collection.find({}).sort("chunk_number", 1)
        async for chunk in cursor:
            # tabela_dados_chunks usa campo 'data'
            for item in chunk.get("data", []) or []:
                # Aplicar filtros de bases/tipos/aging
                if not matches_filters(item, bases_list, tipos_list, aging_list):
                    continue

                # Filtrar por cidades se especificado
                if cidades_list:
                    cidade_destino = get_cidade_destino(item).upper()
                    if cidade_destino not in cidades_list:
                        continue

                # Extrair e validar número do pedido
                numero = get_numero_pedido(item)
                if not numero:
                    continue
                
                # Remover pedidos filhos
                if is_child_pedido(numero):
                    continue
                
                # Deduplicar por raiz numérica
                raiz = extract_raiz_numero(numero)
                if raiz:
                    if raiz in raiz_vistas:
                        continue
                    raiz_vistas.add(raiz)

                # Extrair responsável, marca e base
                responsavel = get_responsavel(item)
                marca = get_marca_assinatura(item).lower()
                base_entrega = get_base_entrega(item)
                
                # Usar chave composta: responsavel + base para identificar motorista único por base
                key_motorista = f"{responsavel}||{base_entrega}" if base_entrega else responsavel
                
                if key_motorista not in stats:
                    stats[key_motorista] = {
                        "responsavel": responsavel,
                        "base": base_entrega,
                        "total": 0,
                        "entregues": 0,
                        "nao_entregues": 0,
                        "entrada_galpao": 0,
                    }

                stats[key_motorista]["total"] += 1
                
                # Classificar status do pedido
                if is_entregue(marca):
                    stats[key_motorista]["entregues"] += 1
                elif is_nao_entregue(marca):
                    stats[key_motorista]["nao_entregues"] += 1
                else:
                    # Não mapeado: considera como não entregue
                    stats[key_motorista]["nao_entregues"] += 1

                total_validos += 1

        data = list(stats.values())
        data.sort(key=lambda x: x["total"], reverse=True)

        return {
            "success": True,
            "data": data,
            "total_responsaveis": len(data),
            "total_pedidos": total_validos,
        }
    except Exception as e:
        logger.error(f"Erro em pedidos-parados: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

