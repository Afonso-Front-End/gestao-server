"""
Rotas para listar dados de Sem Movimentação SC
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
import logging
from app.services.database import get_database
from app.core.collections import COLLECTION_SEM_MOVIMENTACAO_SC_CHUNKS

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Sem Movimentação SC - List"])


@router.get("/list")
async def listar_sem_movimentacao_sc(
    tipo_operacao: str = Query(None, description="Filtrar por tipo de operação (separados por vírgula)"),
    aging: str = Query(None, description="Filtrar por aging (separados por vírgula)"),
    limit: int = Query(50000, description="Limite de registros"),
    skip: int = Query(0, description="Registros para pular")
):
    """
    Lista dados de Sem Movimentação SC filtrados por tipo de operação e aging
    
    Args:
        tipo_operacao: Tipos de operação separados por vírgula (opcional)
        aging: Agings separados por vírgula (opcional)
        limit: Limite de registros
        skip: Registros para pular
    """
    try:
        db = get_database()
        collection = db[COLLECTION_SEM_MOVIMENTACAO_SC_CHUNKS]
        
        # Construir filtros
        match_filters = {}
        
        if tipo_operacao:
            tipos_list = [t.strip() for t in tipo_operacao.split(',') if t.strip()]
            if tipos_list:
                logger.info(f"🔍 Filtrando por tipos de operação: {tipos_list}")
                # Usar regex case-insensitive para melhor matching
                match_filters['data.tipo_ultima_operacao'] = {
                    '$in': tipos_list
                }
        
        if aging:
            agings_list = [a.strip() for a in aging.split(',') if a.strip()]
            if agings_list:
                logger.info(f"🔍 Filtrando por agings: {agings_list}")
                match_filters['data.aging'] = {'$in': agings_list}
        
        logger.info(f"📊 Filtros aplicados: {match_filters}")
        
        # Pipeline de agregação para desempacotar chunks e filtrar
        pipeline = [
            # Filtrar chunks que têm dados
            {'$match': {'data': {'$exists': True, '$ne': []}}},
            # Desempacotar array de dados
            {'$unwind': '$data'},
            # Aplicar filtros nos dados
        ]
        
        if match_filters:
            pipeline.append({'$match': match_filters})
        
        # Adicionar projeção e ordenação
        pipeline.extend([
            {'$project': {
                '_id': 0,
                'remessa': '$data.remessa',
                'nome_base_mais_recente': '$data.nome_base_mais_recente',
                'unidade_responsavel': '$data.unidade_responsavel',
                'base_entrega': '$data.base_entrega',
                'horario_ultima_operacao': '$data.horario_ultima_operacao',
                'tipo_ultima_operacao': '$data.tipo_ultima_operacao',
                'operador_bipe_mais_recente': '$data.operador_bipe_mais_recente',
                'aging': '$data.aging',
                'numero_id': '$data.numero_id'
            }},
            {'$sort': {'remessa': 1}},
            {'$skip': skip},
            {'$limit': limit}
        ])
        
        # Executar agregação
        logger.info(f"📋 Pipeline de agregação: {pipeline}")
        cursor = collection.aggregate(pipeline)
        dados = await cursor.to_list(length=limit)
        logger.info(f"✅ Total de registros retornados: {len(dados)}")
        
        # Contar total de remessas únicas (sem limit)
        count_pipeline = [
            {'$match': {'data': {'$exists': True, '$ne': []}}},
            {'$unwind': '$data'}
        ]
        if match_filters:
            count_pipeline.append({'$match': match_filters})
        # Agrupar por remessa para contar apenas remessas únicas
        count_pipeline.extend([
            {'$group': {
                '_id': '$data.remessa'
            }},
            {'$count': 'total'}
        ])
        
        count_result = await collection.aggregate(count_pipeline).to_list(length=1)
        total = count_result[0]['total'] if count_result else 0
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": dados,
                "total": total,
                "limit": limit,
                "skip": skip
            }
        )
        
    except Exception as e:
        logger.error(f"Erro ao listar Sem Movimentação SC: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/filters")
async def obter_filtros_sem_movimentacao_sc():
    """
    Retorna valores únicos de 'Tipo da última operação' e 'Aging' para popular os selects
    """
    try:
        db = get_database()
        collection = db[COLLECTION_SEM_MOVIMENTACAO_SC_CHUNKS]
        
        # Pipeline para obter valores únicos
        pipeline = [
            {'$match': {'data': {'$exists': True, '$ne': []}}},
            {'$unwind': '$data'},
            {'$group': {
                '_id': None,
                'tipos_operacao': {'$addToSet': '$data.tipo_ultima_operacao'},
                'agings': {'$addToSet': '$data.aging'}
            }},
            {'$project': {
                '_id': 0,
                'tipos_operacao': {
                    '$filter': {
                        'input': '$tipos_operacao',
                        'as': 'tipo',
                        'cond': {'$ne': ['$$tipo', None]}
                    }
                },
                'agings': {
                    '$filter': {
                        'input': '$agings',
                        'as': 'aging',
                        'cond': {'$ne': ['$$aging', None]}
                    }
                }
            }}
        ]
        
        result = await collection.aggregate(pipeline).to_list(length=1)
        
        if result:
            tipos_operacao = sorted([t for t in result[0].get('tipos_operacao', []) if t])
            agings = sorted([a for a in result[0].get('agings', []) if a])
        else:
            tipos_operacao = []
            agings = []
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "tipos_operacao": tipos_operacao,
                "agings": agings
            }
        )
        
    except Exception as e:
        logger.error(f"Erro ao obter filtros: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

