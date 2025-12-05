"""
Rotas para buscar números de pedidos D-1 filtrados por bases
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from app.services.database import get_database
from app.core.collections import COLLECTION_D1_CHUNKS, COLLECTION_D1_BIPAGENS
import logging
import re

logger = logging.getLogger(__name__)

router = APIRouter(tags=["D-1 - Pedidos"])

@router.get("/pedidos")
async def get_d1_pedidos(
    bases: str = Query(..., description="Bases separadas por vírgula"),
    source: str = Query("bipagens", description="Fonte dos dados: 'bipagens' (padrão) ou 'chunks'"),
    tempo_parado: str = Query(None, description="Filtrar por tempo parado (separados por vírgula)")
):
    """
    Retorna os números de pedidos filtrados por bases e tempo parado
    
    Se source='bipagens' (padrão):
    - Busca da coleção D1_BIPAGENS (bipagens em tempo real)
    - Apenas pedidos que estão com motorista (esta_com_motorista=True)
    - Apenas pedidos das bases selecionadas
    - Apenas pedidos com tempo parado selecionado (se fornecido)
    - Apenas pedidos "pais" (remove filhos com sufixo -XXX)
    
    Se source='chunks':
    - Busca da coleção D1_CHUNKS (uploads D1)
    - Apenas pedidos com status "Não entregue"
    - Apenas pedidos das bases selecionadas
    - Apenas pedidos "pais" (remove filhos com sufixo -XXX)
    
    Args:
        bases: String com bases separadas por vírgula (ex: "CCM -SC,ITJ -SC")
        source: Fonte dos dados ('bipagens' ou 'chunks')
        tempo_parado: Tempos parados separados por vírgula (opcional)
        
    Returns:
        Lista de números de pedidos únicos (apenas pais)
    """
    try:
        db = get_database()
        
        # Converter string de bases em lista
        bases_list = [base.strip() for base in bases.split(',') if base.strip()]
        
        if not bases_list:
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "data": [],
                    "total": 0
                }
            )
        
        # Processar filtro de tempo parado
        tempos_list = None
        if tempo_parado:
            tempos_list = [t.strip() for t in tempo_parado.split(',') if t.strip()]
        
        logger.info(f"🔍 Buscando pedidos para bases: {bases_list} (fonte: {source})" + 
                   (f", tempos: {tempos_list}" if tempos_list else ""))
        
        # Buscar de bipagens (padrão) ou chunks
        if source == "bipagens":
            collection = db[COLLECTION_D1_BIPAGENS]
            
            # Construir match query
            match_query = {
                "esta_com_motorista": True,
                "$or": [
                    {"base_entrega": {"$in": bases_list}},
                    {"base_destino": {"$in": bases_list}}
                ]
            }
            
            # Adicionar filtro de tempo parado se fornecido
            if tempos_list:
                match_query["tempo_pedido_parado"] = {"$in": tempos_list}
            
            # Pipeline para buscar pedidos da coleção bipagens
            # IMPORTANTE: Primeiro agrupar por número de pedido para pegar apenas a bipagem mais recente
            pipeline = [
                # Filtrar apenas pedidos com motorista e das bases selecionadas
                {"$match": match_query},
                # Ordenar por número de pedido e tempo de digitalização (mais recente primeiro)
                {"$sort": {
                    "numero_pedido_jms": 1,
                    "tempo_digitalizacao": -1
                }},
                # Agrupar por número de pedido e pegar apenas o primeiro registro (mais recente)
                {"$group": {
                    "_id": "$numero_pedido_jms",
                    # Pegar todos os campos do documento mais recente
                    "doc": {"$first": "$$ROOT"}
                }},
                # Substituir o documento agrupado pelo documento original
                {"$replaceRoot": {"newRoot": "$doc"}},
                # Filtrar apenas valores não-nulos e não-vazios
                {"$match": {
                    "numero_pedido_jms": {"$exists": True, "$ne": None, "$ne": ""}
                }},
                # Converter para string para processamento
                {"$addFields": {
                    "numero_str": {"$toString": "$numero_pedido_jms"}
                }},
                # Filtrar apenas pedidos "pais" (remover filhos)
                {"$match": {
                    "$expr": {
                        "$not": {
                            "$regexMatch": {
                                "input": "$numero_str",
                                "regex": "^-?\\d+-\\d+$"
                            }
                        }
                    }
                }},
                # Projetar apenas o número do pedido
                {"$project": {
                    "_id": 0,
                    "numero_pedido": "$numero_pedido_jms"
                }},
                # Ordenar
                {"$sort": {"numero_pedido": 1}}
            ]
        else:
            # Buscar de chunks (comportamento antigo)
            collection = db[COLLECTION_D1_CHUNKS]
            
            # Contar total de chunks antes da busca
            total_chunks_in_collection = await collection.count_documents({})
            logger.info(f"📦 Total de chunks na coleção: {total_chunks_in_collection}")
            
            # Pipeline para buscar pedidos da coleção chunks
            pipeline = [
                # Desempacotar todos os chunks
                {"$unwind": "$chunk_data"},
                # Filtrar apenas registros das bases selecionadas E com status "Não entregue"
                {"$match": {
                    "chunk_data.Base de entrega": {"$in": bases_list},
                    "chunk_data.Marca de assinatura": {
                        "$regex": "Não entregue",
                        "$options": "i"  # Case insensitive
                    }
                }},
                # Extrair apenas a coluna "Número de pedido JMS"
                {"$project": {
                    "numero_pedido": "$chunk_data.Número de pedido JMS"
                }},
                # Filtrar apenas valores não-nulos e não-vazios
                {"$match": {
                    "numero_pedido": {"$exists": True, "$ne": None, "$ne": ""}
                }},
                # Converter para string para processamento
                {"$addFields": {
                    "numero_str": {"$toString": "$numero_pedido"}
                }},
                # Filtrar apenas pedidos "pais" (remover filhos que têm padrão -XXX)
                {"$match": {
                    "$expr": {
                        "$not": {
                            "$regexMatch": {
                                "input": "$numero_str",
                                "regex": "^-?\\d+-\\d+$"  # Padrão: número-número (ex: 888001152307637-001)
                            }
                        }
                    }
                }},
                # Agrupar por número de pedido para obter valores únicos
                {"$group": {
                    "_id": "$numero_pedido"
                }},
                # Ordenar
                {"$sort": {"_id": 1}},
                # Projetar apenas o número do pedido
                {"$project": {
                    "_id": 0,
                    "numero_pedido": "$_id"
                }}
            ]
        
        numeros_pedidos = []
        total_processed = 0
        
        # Executar aggregation e processar resultados
        async for doc in collection.aggregate(pipeline):
            total_processed += 1
            numero = doc.get('numero_pedido', '')
            # Converter para string e limpar
            numero_str = str(numero).strip() if numero else ''
            
            if numero_str:
                # Verificação adicional: garantir que não é um pedido filho
                # Pedidos filhos têm formato: número-número (ex: 888001152307637-001)
                # Pedidos pais não têm hífen seguido de números
                if not re.match(r'^-?\d+-\d+$', numero_str):
                    numeros_pedidos.append(numero_str)
        
        logger.info(
            f"✅ Processamento concluído:\n"
            f"   - Fonte: {source}\n"
            f"   - Registros processados pelo pipeline: {total_processed:,}\n"
            f"   - Pedidos PAI encontrados: {len(numeros_pedidos):,}\n"
            f"   - Bases filtradas: {len(bases_list)}"
        )
        
        response_content = {
            "success": True,
            "data": numeros_pedidos,
            "total": len(numeros_pedidos),
            "bases": bases_list,
            "source": source,
            "filters": {
                "removed_children": True
            }
        }
        
        if source == "chunks":
            response_content["filters"]["status"] = "Não entregue"
            response_content["note"] = "Busca realizada em TODOS os chunks da coleção. Apenas pedidos pais com status 'Não entregue' retornados (filhos com sufixo -XXX foram removidos)"
        else:
            response_content["filters"]["esta_com_motorista"] = True
            response_content["note"] = "Busca realizada na coleção de bipagens em tempo real. Apenas pedidos pais com motorista retornados (filhos com sufixo -XXX foram removidos)"
        
        return JSONResponse(
            status_code=200,
            content=response_content
        )
    except Exception as e:
        logger.error(f"Erro ao buscar pedidos D-1: {e}")
        raise HTTPException(status_code=500, detail=str(e))

