"""
Rotas para upload e listagem de dados de bipagens em tempo real
"""
from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from fastapi.responses import JSONResponse
import logging
import re
from datetime import datetime
from app.modules.d1.services.bipagens_processor import BipagensProcessor
from app.services.database import get_database
from app.core.collections import COLLECTION_D1_BIPAGENS
from bson import ObjectId

logger = logging.getLogger(__name__)

router = APIRouter(tags=["D-1 - Bipagens"])

processor = BipagensProcessor()

@router.post("/bipagens/upload")
async def upload_bipagens(file: UploadFile = File(...)):
    """
    Upload de arquivo Excel com dados de bipagens em tempo real
    
    Validações:
    - Deduplica por número de pedido (pega data mais recente)
    - Cruza dados com d1_chunks
    - Calcula tempo de pedido parado
    """
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="Nenhum arquivo foi enviado")
        
        if not file.filename.lower().endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400,
                detail="Formato de arquivo não suportado. Use .xlsx ou .xls"
            )
        
        # Ler conteúdo do arquivo
        file_content = await file.read()
        
        if len(file_content) == 0:
            raise HTTPException(status_code=400, detail="Arquivo vazio")
        
        # Processar arquivo
        result = await processor.process_file(file_content, file.filename)
        
        return JSONResponse(
            status_code=200,
            content=result
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao fazer upload de bipagens: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/bipagens")
async def listar_bipagens(
    base: str = Query(None, description="Filtrar por base (separadas por vírgula)"),
    tempo_parado: str = Query(None, description="Filtrar por tempo parado (separados por vírgula)"),
    limit: int = Query(100, description="Limite de registros"),
    skip: int = Query(0, description="Registros para pular")
):
    """
    Lista dados de bipagens processados
    
    Args:
        base: Filtrar por base (opcional, múltiplas separadas por vírgula)
        tempo_parado: Filtrar por tempo parado (opcional, múltiplos separados por vírgula)
        limit: Limite de registros
        skip: Registros para pular
    """
    try:
        db = get_database()
        collection = db[COLLECTION_D1_BIPAGENS]
        
        # Pipeline de agregação para pegar apenas a bipagem mais recente de cada pedido
        # IMPORTANTE: Primeiro agrupar por número de pedido para pegar apenas a bipagem mais recente
        
        # Construir match inicial
        match_query = {}
        
        if base:
            bases_list = [b.strip() for b in base.split(',') if b.strip()]
            if bases_list:
                # Filtrar por base_entrega OU base_destino
                match_query['$or'] = [
                    {'base_entrega': {'$in': bases_list}},
                    {'base_destino': {'$in': bases_list}}
                ]
        
        if tempo_parado:
            tempos_list = [t.strip() for t in tempo_parado.split(',') if t.strip()]
            if tempos_list:
                match_query['tempo_pedido_parado'] = {'$in': tempos_list}
        
        # Pipeline de agregação
        pipeline = [
            # Primeiro match: aplicar filtros iniciais (base, tempo_parado)
            {'$match': match_query} if match_query else {'$match': {}},
            
            # Ordenar por número de pedido e tempo de digitalização (mais recente primeiro)
            {'$sort': {
                'numero_pedido_jms': 1,
                'tempo_digitalizacao': -1
            }},
            
            # Agrupar por número de pedido e pegar apenas o primeiro registro (mais recente)
            {'$group': {
                '_id': '$numero_pedido_jms',
                # Pegar todos os campos do documento mais recente
                'doc': {'$first': '$$ROOT'}
            }},
            
            # Substituir o documento agrupado pelo documento original
            {'$replaceRoot': {'newRoot': '$doc'}},
            
            # Filtrar apenas pedidos que estão com motorista (não digitalizadores)
            {'$match': {
                'esta_com_motorista': True
            }},
            
            # Ordenar por updated_at (mais recente primeiro)
            {'$sort': {'updated_at': -1}},
        ]
        
        # Contar total (executar pipeline sem limit/skip)
        count_pipeline = pipeline + [{'$count': 'total'}]
        total = 0
        async for doc in collection.aggregate(count_pipeline):
            total = doc.get('total', 0)
            break
        
        # Aplicar skip e limit
        pipeline.extend([
            {'$skip': skip},
            {'$limit': limit}
        ])
        
        # Executar pipeline
        documentos = []
        async for doc in collection.aggregate(pipeline):
            # Converter ObjectId para string
            doc['_id'] = str(doc['_id'])
            # Converter datetime para string
            if 'tempo_digitalizacao' in doc and doc['tempo_digitalizacao']:
                if isinstance(doc['tempo_digitalizacao'], datetime):
                    doc['tempo_digitalizacao'] = doc['tempo_digitalizacao'].isoformat()
            if 'created_at' in doc and doc['created_at']:
                if isinstance(doc['created_at'], datetime):
                    doc['created_at'] = doc['created_at'].isoformat()
            if 'updated_at' in doc and doc['updated_at']:
                if isinstance(doc['updated_at'], datetime):
                    doc['updated_at'] = doc['updated_at'].isoformat()
            
            documentos.append(doc)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": documentos,
                "total": total,
                "limit": limit,
                "skip": skip
            }
        )
        
    except Exception as e:
        logger.error(f"Erro ao listar bipagens: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/bipagens/motoristas")
async def listar_motoristas_agrupados(
    base: str = Query(None, description="Filtrar por base (separadas por vírgula)"),
    tempo_parado: str = Query(None, description="Filtrar por tempo parado (separados por vírgula)"),
    cidade: str = Query(None, description="Filtrar por cidade destino (separadas por vírgula)")
):
    """
    Lista motoristas agrupados com contagem de pedidos
    
    Args:
        base: Filtrar por base (opcional, múltiplas separadas por vírgula)
        tempo_parado: Filtrar por tempo parado (opcional, múltiplos separados por vírgula)
        cidade: Filtrar por cidade destino (opcional, múltiplas separadas por vírgula)
    """
    try:
        db = get_database()
        collection = db[COLLECTION_D1_BIPAGENS]
        
        # Construir query
        match_query = {}
        if base:
            bases_list = [b.strip() for b in base.split(',') if b.strip()]
            if bases_list:
                # Filtrar por base_entrega OU base_escaneamento
                match_query['$or'] = [
                    {'base_entrega': {'$in': bases_list}},
                    {'base_escaneamento': {'$in': bases_list}}
                ]
        
        if tempo_parado:
            tempos_list = [t.strip() for t in tempo_parado.split(',') if t.strip()]
            if tempos_list:
                match_query['tempo_pedido_parado'] = {'$in': tempos_list}
        
        if cidade:
            cidades_list = [c.strip() for c in cidade.split(',') if c.strip()]
            if cidades_list:
                match_query['cidade_destino'] = {'$in': cidades_list}
        
        # Pipeline de agregação
        # IMPORTANTE: Primeiro agrupar por número de pedido para pegar apenas a bipagem mais recente
        # Depois agrupar por motorista para contar os pedidos
        
        pipeline = [
            # Primeiro match: aplicar filtros iniciais (base, tempo_parado, cidade)
            {'$match': match_query} if match_query else {'$match': {}},
            
            # Ordenar por número de pedido e tempo de digitalização (mais recente primeiro)
            {'$sort': {
                'numero_pedido_jms': 1,
                'tempo_digitalizacao': -1
            }},
            
            # Agrupar por número de pedido e pegar apenas o primeiro registro (mais recente)
            {'$group': {
                '_id': '$numero_pedido_jms',
                # Pegar todos os campos do documento mais recente
                'doc': {'$first': '$$ROOT'}
            }},
            
            # Substituir o documento agrupado pelo documento original
            {'$replaceRoot': {'newRoot': '$doc'}},
            
            # Agora filtrar apenas pedidos que estão com motorista (esta_com_motorista = True)
            # E que têm responsavel_entrega preenchido
            {'$match': {
                'responsavel_entrega': {'$exists': True, '$ne': '', '$ne': None},
                'esta_com_motorista': True
            }},
            
            # Agora agrupar por responsavel_entrega (motorista) para contar pedidos
            {'$group': {
                '_id': '$responsavel_entrega',
                'base_entrega': {'$first': '$base_entrega'},
                'base_destino': {'$first': '$base_destino'},
                'total_pedidos': {'$sum': 1},
                'pedidos': {'$push': {
                    'marca_assinatura': '$marca_assinatura'
                }},
                'esta_com_motorista': {'$first': '$esta_com_motorista'}
            }},
            {'$addFields': {
                'total_entregues': {
                    '$size': {
                        '$filter': {
                            'input': '$pedidos',
                            'as': 'pedido',
                            'cond': {
                                '$or': [
                                    {'$regexMatch': {'input': {'$toLower': {'$ifNull': ['$$pedido.marca_assinatura', '']}}, 'regex': 'assinatura de devolução|recebimento com assinatura normal'}},
                                    {'$eq': [{'$toLower': {'$ifNull': ['$$pedido.marca_assinatura', '']}}, 'entregue']}
                                ]
                            }
                        }
                    }
                },
                'total_nao_entregues': {
                    '$size': {
                        '$filter': {
                            'input': '$pedidos',
                            'as': 'pedido',
                            'cond': {
                                '$regexMatch': {'input': {'$toLower': {'$ifNull': ['$$pedido.marca_assinatura', '']}}, 'regex': 'não entregue|nao entregue|não entregues|nao entregues'}
                            }
                        }
                    }
                }
            }},
            {'$sort': {'total_pedidos': -1}},
            {'$project': {
                '_id': 0,
                'motorista': '$_id',
                'base_entrega': {'$ifNull': ['$base_entrega', '$base_destino']},
                'total_pedidos': 1,
                'total_entregues': {'$ifNull': ['$total_entregues', 0]},
                'total_nao_entregues': {'$ifNull': ['$total_nao_entregues', 0]},
                'esta_com_motorista': {'$ifNull': ['$esta_com_motorista', False]}
            }}
        ]
        
        motoristas = []
        async for doc in collection.aggregate(pipeline):
            # Os valores de total_entregues e total_nao_entregues já foram calculados pelo pipeline
            # Garantir que existam e sejam números válidos
            doc['total_entregues'] = doc.get('total_entregues', 0) or 0
            doc['total_nao_entregues'] = doc.get('total_nao_entregues', 0) or 0
            
            motoristas.append(doc)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": motoristas,
                "total": len(motoristas)
            }
        )
        
    except Exception as e:
        logger.error(f"Erro ao listar motoristas agrupados: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/bipagens/cidades")
async def listar_cidades_disponiveis(
    base: str = Query(None, description="Filtrar por base (separadas por vírgula)")
):
    """
    Lista cidades únicas disponíveis baseadas nas bases selecionadas
    
    Args:
        base: Filtrar por base (opcional, múltiplas separadas por vírgula)
    """
    try:
        db = get_database()
        collection = db[COLLECTION_D1_BIPAGENS]
        
        # Construir query
        match_query = {}
        if base:
            bases_list = [b.strip() for b in base.split(',') if b.strip()]
            if bases_list:
                match_query['$or'] = [
                    {'base_entrega': {'$in': bases_list}},
                    {'base_escaneamento': {'$in': bases_list}}
                ]
        
        # Pipeline para buscar cidades únicas
        pipeline = [
            {'$match': match_query},
            {'$match': {
                'cidade_destino': {'$exists': True, '$ne': '', '$ne': None}
            }},
            {'$group': {
                '_id': '$cidade_destino'
            }},
            {'$sort': {'_id': 1}},
            {'$project': {
                '_id': 0,
                'cidade': '$_id'
            }}
        ]
        
        cidades = []
        async for doc in collection.aggregate(pipeline):
            cidade = doc.get('cidade', '').strip()
            if cidade:
                cidades.append(cidade)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": cidades,
                "total": len(cidades)
            }
        )
        
    except Exception as e:
        logger.error(f"Erro ao listar cidades disponíveis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.post("/bipagens/atualizar-marca-assinatura")
async def atualizar_marca_assinatura(file: UploadFile = File(...)):
    """
    Atualiza a marca de assinatura dos pedidos baseado em um arquivo Excel
    
    Processa o arquivo pedido por pedido:
    - Lê o arquivo Excel
    - Para cada pedido, busca no banco pelo número de pedido
    - Se encontrar, atualiza apenas a marca_assinatura
    - Funciona mesmo se o arquivo tiver apenas alguns pedidos
    
    Args:
        file: Arquivo Excel com colunas: "Número de pedido JMS" e "Marca de assinatura"
    """
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="Nenhum arquivo foi enviado")
        
        if not file.filename.lower().endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400,
                detail="Formato de arquivo não suportado. Use .xlsx ou .xls"
            )
        
        # Ler conteúdo do arquivo
        file_content = await file.read()
        
        if len(file_content) == 0:
            raise HTTPException(status_code=400, detail="Arquivo vazio")
        
        # Processar arquivo usando openpyxl (já usado no projeto)
        from io import BytesIO
        import openpyxl
        
        # Ler Excel
        workbook = openpyxl.load_workbook(BytesIO(file_content), data_only=True)
        sheet = workbook.active
        
        # Ler cabeçalhos da primeira linha
        headers = []
        for cell in sheet[1]:
            headers.append(str(cell.value).strip() if cell.value else '')
        
        # Procurar coluna de número de pedido (várias possibilidades)
        numero_pedido_col_idx = None
        for idx, header in enumerate(headers):
            header_lower = str(header).lower()
            if 'número de pedido' in header_lower or 'numero de pedido' in header_lower or 'nº do pedido' in header_lower or 'numero_pedido' in header_lower:
                numero_pedido_col_idx = idx
                break
        
        if numero_pedido_col_idx is None:
            raise HTTPException(
                status_code=400,
                detail="Coluna de número de pedido não encontrada. Procure por: 'Número de pedido JMS', 'Nº DO PEDIDO', 'NUMERO_PEDIDO'"
            )
        
        # Procurar coluna de marca de assinatura
        marca_assinatura_col_idx = None
        for idx, header in enumerate(headers):
            header_lower = str(header).lower()
            if 'marca de assinatura' in header_lower or 'marca_assinatura' in header_lower or 'status' in header_lower:
                marca_assinatura_col_idx = idx
                break
        
        if marca_assinatura_col_idx is None:
            raise HTTPException(
                status_code=400,
                detail="Coluna de marca de assinatura não encontrada. Procure por: 'Marca de assinatura', 'Status'"
            )
        
        db = get_database()
        collection = db[COLLECTION_D1_BIPAGENS]
        
        total_processados = 0
        total_atualizados = 0
        total_nao_encontrados = 0
        erros = []
        
        # Processar cada linha do arquivo (começando da linha 2, pois linha 1 é cabeçalho)
        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=False), start=2):
            try:
                numero_pedido_cell = row[numero_pedido_col_idx]
                marca_assinatura_cell = row[marca_assinatura_col_idx]
                
                numero_pedido = str(numero_pedido_cell.value).strip() if numero_pedido_cell.value else None
                marca_assinatura = str(marca_assinatura_cell.value).strip() if marca_assinatura_cell.value else None
                
                if not numero_pedido or numero_pedido == 'None' or numero_pedido == '':
                    continue
                
                if not marca_assinatura or marca_assinatura == 'None' or marca_assinatura == '':
                    continue
                
                # Remover pedidos filhos (formato: 888001152307637-001, 888001152307637-002, etc.)
                # Verificar se é pedido filho (tem hífen seguido de números, ponto seguido de números, ou letra no final)
                numero_pedido_str = str(numero_pedido)
                # Padrões de pedidos filhos: -001, -002, .001, .002, _001, _002, ou letra no final (A, B, C)
                is_child = bool(
                    re.search(r"\.\d+$", numero_pedido_str) or 
                    re.search(r"-\d+$", numero_pedido_str) or 
                    re.search(r"_\d+$", numero_pedido_str) or 
                    re.search(r"[A-Za-z]$", numero_pedido_str)
                )
                if is_child:
                    continue  # Pular pedidos filhos
                
                total_processados += 1
                
                # Buscar pedido no banco
                pedido_existente = await collection.find_one({'numero_pedido_jms': numero_pedido})
                
                if pedido_existente:
                    # Atualizar apenas a marca_assinatura
                    await collection.update_one(
                        {'numero_pedido_jms': numero_pedido},
                        {
                            '$set': {
                                'marca_assinatura': marca_assinatura,
                                'updated_at': datetime.now()
                            }
                        }
                    )
                    total_atualizados += 1
                else:
                    total_nao_encontrados += 1
                    
            except Exception as e:
                erros.append(f"Linha {row_idx}: {str(e)}")
                logger.error(f"Erro ao processar linha {row_idx}: {e}")
                continue
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Atualização concluída",
                "total_processados": total_processados,
                "total_atualizados": total_atualizados,
                "total_nao_encontrados": total_nao_encontrados,
                "erros": erros[:10] if erros else [],  # Limitar a 10 erros
                "total_erros": len(erros)
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao atualizar marca de assinatura: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/bipagens/motorista/all-status")
async def obter_todos_status_d1():
    """
    Obtém todos os status salvos dos motoristas D1 (para carregar observações ao iniciar)
    """
    try:
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        collection_name = "motoristas_status_d1"
        collection = db[collection_name]
        
        # Buscar todos os status de motoristas D1
        cursor = collection.find({})
        statuses = []
        
        async for doc in cursor:
            # Remover _id do MongoDB para serialização
            doc.pop('_id', None)
            # Garantir que tenha o campo responsavel (pode ser motorista ou responsavel)
            if 'motorista' in doc and 'responsavel' not in doc:
                doc['responsavel'] = doc['motorista']
            # Garantir que tenha o campo observacao (mesmo que vazio)
            if 'observacao' not in doc:
                doc['observacao'] = ''
            statuses.append(doc)
        
        return {
            "success": True,
            "statuses": statuses,
            "total": len(statuses)
        }
        
    except Exception as e:
        logger.error(f"Erro ao obter todos os status D1: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/bipagens/motorista/{motorista}")
async def listar_pedidos_motorista(
    motorista: str,
    base: str = Query(None, description="Filtrar por base (separadas por vírgula)"),
    tempo_parado: str = Query(None, description="Filtrar por tempo parado (separados por vírgula)"),
    status: str = Query(None, description="Filtrar por status: 'entregue' ou 'nao_entregue'")
):
    """
    Lista todos os pedidos de um motorista específico
    IMPORTANTE: Retorna apenas pedidos onde o motorista é o responsável na BIPAGEM MAIS RECENTE
    
    Args:
        motorista: Nome do motorista (URL encoded)
        base: Filtrar por base (opcional)
        tempo_parado: Filtrar por tempo parado (opcional)
        status: Filtrar por status - 'entregue' ou 'nao_entregue' (opcional)
    """
    try:
        import urllib.parse
        motorista_decoded = urllib.parse.unquote(motorista)
        
        db = get_database()
        collection = db[COLLECTION_D1_BIPAGENS]
        
        # Construir match inicial - buscar todos os pedidos (sem filtrar por motorista ainda)
        match_stage = {}
        
        # Construir condições de base
        if base:
            bases_list = [b.strip() for b in base.split(',') if b.strip()]
            if bases_list:
                # Filtrar por base_entrega OU base_destino
                match_stage['$or'] = [
                    {'base_entrega': {'$in': bases_list}},
                    {'base_destino': {'$in': bases_list}}
                ]
        
        # Pipeline de agregação para pegar apenas a bipagem mais recente de cada pedido
        pipeline = [
            # Primeiro match: aplicar filtros de base (se houver)
            {'$match': match_stage} if match_stage else {'$match': {}},
            
            # Ordenar por número de pedido e tempo de digitalização (mais recente primeiro)
            {'$sort': {
                'numero_pedido_jms': 1,
                'tempo_digitalizacao': -1
            }},
            
            # Agrupar por número de pedido e pegar apenas o primeiro registro (mais recente)
            {'$group': {
                '_id': '$numero_pedido_jms',
                # Pegar todos os campos do documento mais recente
                'doc': {'$first': '$$ROOT'}
            }},
            
            # Substituir o documento agrupado pelo documento original
            {'$replaceRoot': {'newRoot': '$doc'}},
            
            # Agora filtrar apenas os que têm o motorista correto na bipagem mais recente
            {'$match': {
                'responsavel_entrega': motorista_decoded,
                'esta_com_motorista': True
            }},
            
            # Aplicar filtros adicionais (tempo_parado e status)
        ]
        
        # Adicionar filtro de tempo_parado se fornecido
        if tempo_parado:
            tempos_list = [t.strip() for t in tempo_parado.split(',') if t.strip()]
            if tempos_list:
                pipeline.append({
                    '$match': {'tempo_pedido_parado': {'$in': tempos_list}}
                })
        
        # Filtrar por status (entregue ou não entregue)
        if status:
            if status.lower() == 'entregue':
                # Pedidos entregues: "Recebimento com assinatura normal", "Assinatura de devolução", ou "entregue"
                pipeline.append({
                    '$match': {
                        'marca_assinatura': {
                            '$regex': 'recebimento com assinatura normal|assinatura de devolução|^entregue$',
                            '$options': 'i'
                        }
                    }
                })
            elif status.lower() == 'nao_entregue':
                # Pedidos não entregues
                pipeline.append({
                    '$match': {
                        'marca_assinatura': {
                            '$regex': 'não entregue|nao entregue',
                            '$options': 'i'
                        }
                    }
                })
        
        # Ordenar resultado final por tempo de digitalização (mais recente primeiro)
        pipeline.append({'$sort': {'tempo_digitalizacao': -1}})
        
        # Executar pipeline
        documentos = []
        async for doc in collection.aggregate(pipeline):
            # Converter ObjectId para string
            doc['_id'] = str(doc['_id'])
            # Converter datetime para string
            if 'tempo_digitalizacao' in doc and doc['tempo_digitalizacao']:
                if isinstance(doc['tempo_digitalizacao'], datetime):
                    doc['tempo_digitalizacao'] = doc['tempo_digitalizacao'].isoformat()
            if 'created_at' in doc and doc['created_at']:
                if isinstance(doc['created_at'], datetime):
                    doc['created_at'] = doc['created_at'].isoformat()
            if 'updated_at' in doc and doc['updated_at']:
                if isinstance(doc['updated_at'], datetime):
                    doc['updated_at'] = doc['updated_at'].isoformat()
            
            documentos.append(doc)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "data": documentos,
                "total": len(documentos),
                "motorista": motorista_decoded
            }
        )
        
    except Exception as e:
        logger.error(f"Erro ao listar pedidos do motorista: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.delete("/bipagens/clear-all")
async def clear_all_bipagens():
    """
    Deleta todos os dados da coleção d1_bipagens
    """
    try:
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Não foi possível conectar ao banco de dados")
        
        collection = db[COLLECTION_D1_BIPAGENS]
        
        # Contar antes da exclusão
        count_before = await collection.count_documents({})
        
        # Deletar todos os documentos
        result = await collection.delete_many({})
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Todos os dados de bipagens foram deletados com sucesso",
                "deleted_count": result.deleted_count,
                "previous_count": count_before
            }
        )
        
    except Exception as e:
        logger.error(f"Erro ao deletar dados de bipagens: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao deletar dados de bipagens: {str(e)}"
        )

@router.post("/bipagens/motorista/{motorista}/status")
async def salvar_status_motorista(
    motorista: str,
    status_data: dict
):
    """
    Salva o status de um motorista (OK, NAO RETORNOU POSSIVEL EXTRAVIO, PENDENTE, NUMERO ERRADO OU SEM DDD OU INCORRETO, NAO CONTATEI)
    """
    try:
        from datetime import datetime
        
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        # Usar coleção específica para status dos motoristas D1
        collection_name = "motoristas_status_d1"
        collection = db[collection_name]
        
        status_value = status_data.get("status")  # Pode ser 'OK', 'NAO RETORNOU POSSIVEL EXTRAVIO', 'PENDENTE', 'NUMERO ERRADO OU SEM DDD OU INCORRETO' ou null
        motorista_value = status_data.get("motorista") or motorista
        base = status_data.get("base") or ""
        
        # Buscar status existente usando chave composta (motorista + base)
        if base:
            query = {"responsavel": motorista_value, "base": base}
        else:
            query = {
                "responsavel": motorista_value,
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
                'OK',
                'NAO RETORNOU POSSIVEL EXTRAVIO',
                'PENDENTE',
                'NUMERO ERRADO OU SEM DDD OU INCORRETO',
                'NAO CONTATEI'
            ]
            if status_value not in STATUS_VALIDOS:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Status inválido: {status_value}. Valores permitidos: {', '.join(STATUS_VALIDOS)}"
                )
            
            # Obter observação se fornecida (sempre incluir campo, mesmo que vazio, para permitir remoção)
            observacao = status_data.get("observacao", "")
            
            # Atualizar ou criar documento com chave composta (motorista + base)
            doc = {
                "responsavel": motorista_value,
                "base": base,
                "status": status_value,
                "observacao": observacao,  # Sempre incluir campo observacao (pode ser vazio para remover)
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
        logger.error(f"Erro ao salvar status do motorista: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.post("/bipagens/table-config/{table_id}")
async def salvar_config_tabela(table_id: str, config_data: dict):
    """
    Salva a configuração de uma tabela (colunas, ordem, visibilidade, estilos)
    """
    try:
        from datetime import datetime
        
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        collection_name = "table_configs"
        collection = db[collection_name]
        
        config = config_data.get("config", {})
        
        # Buscar configuração existente
        existing = await collection.find_one({"table_id": table_id})
        
        doc = {
            "table_id": table_id,
            "config": config,
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
            "message": f"Configuração {result_status} com sucesso",
            "table_id": table_id
        }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao salvar configuração da tabela: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/bipagens/table-config/{table_id}")
async def obter_config_tabela(table_id: str):
    """
    Obtém a configuração de uma tabela
    """
    try:
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        collection_name = "table_configs"
        collection = db[collection_name]
        
        config_doc = await collection.find_one({"table_id": table_id})
        
        if config_doc:
            return {
                "success": True,
                "config": config_doc.get("config", {}),
                "table_id": table_id
            }
        else:
            return {
                "success": True,
                "config": None,
                "table_id": table_id
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao obter configuração da tabela: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.delete("/bipagens/table-config/{table_id}")
async def deletar_config_tabela(table_id: str):
    """
    Deleta a configuração de uma tabela
    """
    try:
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        collection_name = "table_configs"
        collection = db[collection_name]
        
        result = await collection.delete_one({"table_id": table_id})
        
        if result.deleted_count > 0:
            return {
                "success": True,
                "message": "Configuração deletada com sucesso",
                "table_id": table_id
            }
        else:
            return {
                "success": True,
                "message": "Configuração não encontrada",
                "table_id": table_id
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao deletar configuração da tabela: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/bipagens/motorista/{motorista}/status")
async def obter_status_motorista(motorista: str, base: str | None = None):
    """
    Obtém o status de um motorista usando chave composta (motorista + base)
    """
    try:
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        collection_name = "motoristas_status_d1"
        collection = db[collection_name]
        
        # Buscar usando chave composta (motorista + base)
        if base:
            query = {"responsavel": motorista, "base": base}
        else:
            # Se não tiver base, buscar apenas por motorista sem base ou com base vazia
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
                "motorista": doc.get("responsavel"),
                "base": doc.get("base"),
                "observacao": doc.get("observacao", ""),
                "updated_at": doc.get("updated_at")
            }
        else:
            return {
                "success": True,
                "status": None,
                "motorista": motorista,
                "base": base,
                "message": "Nenhum status encontrado"
            }
            
    except Exception as e:
        logger.error(f"Erro ao obter status do motorista: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

