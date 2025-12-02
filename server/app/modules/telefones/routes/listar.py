"""
Rota para listar registros da coleção de telefones
"""
from fastapi import APIRouter, HTTPException, Query
import logging
import urllib.parse
import re
from bson import ObjectId
from app.core.collections import COLLECTION_TELEFONES
from app.services.database import db

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Lista de Telefones - Listar"])

def normalizar_hub(hub: str) -> str:
    """
    Normaliza HUB/base para o formato padrão "SIGLA -SC" (com espaço antes do hífen).
    """
    if not hub:
        return hub
    
    hub_original = str(hub).strip().upper()
    
    # Pattern 1: "CD BNU 001" → "BNU -SC"
    match_cd = re.match(r'CD\s+([A-Z]{2,4})\s+\d+', hub_original)
    if match_cd:
        sigla = match_cd.group(1)
        return f"{sigla} -SC"
    
    # Pattern 2: "BNU-SC" ou "ITJ-SC" → "BNU -SC" ou "ITJ -SC"
    match_sem_espaco = re.match(r'^([A-Z]{2,4})-SC$', hub_original)
    if match_sem_espaco:
        sigla = match_sem_espaco.group(1)
        return f"{sigla} -SC"
    
    # Pattern 3: "BNU- SC" ou "ITJ- SC" → "BNU -SC" ou "ITJ -SC"
    match_espaco_depois = re.match(r'^([A-Z]{2,4})-\s+SC$', hub_original)
    if match_espaco_depois:
        sigla = match_espaco_depois.group(1)
        return f"{sigla} -SC"
    
    # Pattern 4: Fix spacing "BNU - SC" → "BNU -SC"
    if re.search(r'\s+-\s+', hub_original):
        return re.sub(r'\s+-\s+', ' -', hub_original)
    
    # Pattern 5: "BNU -SC" (já está correto)
    match_com_espaco = re.match(r'^([A-Z]{2,4})\s+-SC$', hub_original)
    if match_com_espaco:
        sigla = match_com_espaco.group(1)
        return f"{sigla} -SC"
    
    return hub_original

def convert_to_dict(obj):
    """Converte ObjectId para string para serialização JSON"""
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if isinstance(value, ObjectId):
                result[key] = str(value)
            elif isinstance(value, dict):
                result[key] = convert_to_dict(value)
            elif isinstance(value, list):
                result[key] = [convert_to_dict(item) if isinstance(item, dict) else item for item in value]
            else:
                result[key] = value
        return result
    return obj

@router.get("/listas")
async def listar_todas_listas(limite: int = 50, pular: int = 0):
    """
    Lista registros da coleção de telefones suportando dois formatos:
    - Documentos planos (um registro por documento)
    - Documentos legados com array 'dados_processados'

    A paginação é aplicada após o achatamento dos itens.
    """
    try:
        logger.info(f"[lista-telefones] GET /listas params -> limite={limite}, pular={pular}")
        collection = db.database[COLLECTION_TELEFONES]

        # Colunas esperadas para o frontend
        colunas_necessarias = ['Data', 'Motorista', 'Status', 'Cidade', 'HUB', 'Contato']

        # Buscar documentos e achatar
        docs = await collection.find({}).to_list(length=10_000)
        total_documentos = len(docs)

        legacy_docs = 0
        legacy_itens = 0
        flat_docs = 0

        itens_achatados = []
        for doc in docs:
            dados_proc = doc.get('dados_processados')
            if isinstance(dados_proc, list) and dados_proc:
                legacy_docs += 1
                legacy_itens += len(dados_proc)
                for sub in dados_proc:
                    itens_achatados.append(sub)
            else:
                flat_docs += 1
                itens_achatados.append(doc)

        total_itens_achatados = len(itens_achatados)
        logger.info(
            f"[lista-telefones] docs={total_documentos}, legacy_docs={legacy_docs}, flat_docs={flat_docs}, "
            f"legacy_itens={legacy_itens}, itens_achatados={total_itens_achatados}"
        )

        # Paginação após achatar
        inicio = max(pular, 0)
        fim = inicio + max(limite, 0)
        itens_pagina = itens_achatados[inicio:fim]
        logger.info(
            f"[lista-telefones] paginação -> inicio={inicio}, fim={fim}, itens_pagina={len(itens_pagina)}"
        )

        registros = []
        for item in itens_pagina:
            item_filtrado = {c: item.get(c, '') for c in colunas_necessarias}
            registros.append(item_filtrado)

        response = {
            "registros": registros,
            "total_registros": total_itens_achatados,
            "limite": limite,
            "pular": pular,
            # Debug simples (remover depois)
            "total_documentos": total_documentos,
            "total_itens_achatados": total_itens_achatados,
            "legacy_docs": legacy_docs,
            "flat_docs": flat_docs,
            "legacy_itens": legacy_itens
        }
        return response
    except Exception as e:
        logger.error(f"Erro ao listar registros: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {e}")

@router.get("/bases")
async def listar_bases():
    """
    Lista todas as bases únicas da coleção de telefones
    """
    try:
        collection = db.database[COLLECTION_TELEFONES]
        docs = await collection.find({}).to_list(length=10_000)
        
        bases_unicas = set()
        for doc in docs:
            dados_proc = doc.get('dados_processados')
            if isinstance(dados_proc, list) and dados_proc:
                for sub in dados_proc:
                    hub = sub.get('HUB', '')
                    if hub:
                        bases_unicas.add(normalizar_hub(str(hub)))
            else:
                hub = doc.get('HUB', '')
                if hub:
                    bases_unicas.add(normalizar_hub(str(hub)))
        
        bases_ordenadas = sorted(list(bases_unicas))
        
        return {
            "success": True,
            "bases": bases_ordenadas,
            "total": len(bases_ordenadas)
        }
    except Exception as e:
        logger.error(f"Erro ao listar bases: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/bases/{base}/motoristas")
async def listar_motoristas_por_base(
    base: str,
    busca: str = Query('', description="Busca por nome do motorista ou número de telefone")
):
    """
    Lista todos os motoristas de uma base específica, sem paginação
    """
    try:
        base_decoded = urllib.parse.unquote(base)
        base_normalized = normalizar_hub(base_decoded)
        
        collection = db.database[COLLECTION_TELEFONES]
        docs = await collection.find({}).to_list(length=10_000)
        
        itens_achatados = []
        for doc in docs:
            dados_proc = doc.get('dados_processados')
            if isinstance(dados_proc, list) and dados_proc:
                for sub in dados_proc:
                    itens_achatados.append(sub)
            else:
                itens_achatados.append(doc)
        
        motoristas_base = []
        colunas_para_remover = ['_id', 'timestamp', 'origem']
        
        for item in itens_achatados:
            hub = normalizar_hub(str(item.get('HUB', '')))
            if hub == base_normalized:
                item_dict = convert_to_dict(item) if isinstance(item, dict) else item
                for coluna in colunas_para_remover:
                    item_dict.pop(coluna, None)
                motoristas_base.append(item_dict)
        
        if busca:
            busca_lower = busca.lower().strip()
            motoristas_filtrados = []
            for item in motoristas_base:
                try:
                    motorista = str(item.get('Motorista', '')).lower()
                    contato = str(item.get('Contato', '')).replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
                    busca_sem_formatacao = busca_lower.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
                    
                    if busca_lower in motorista or busca_sem_formatacao in contato:
                        motoristas_filtrados.append(item)
                except Exception as e:
                    logger.warning(f"Erro ao processar item na busca: {e}")
                    continue
            motoristas_base = motoristas_filtrados
        
        motoristas_base.sort(key=lambda x: str(x.get('Motorista', '')).lower())
        
        return {
            "success": True,
            "base": base_normalized,
            "motoristas": motoristas_base,
            "total": len(motoristas_base),
            "busca": busca if busca else None
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao listar motoristas por base: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

