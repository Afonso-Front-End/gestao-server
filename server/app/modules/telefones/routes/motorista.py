"""
Rotas relacionadas ao motorista (buscar e gerenciar telefones)
"""
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime
import logging
import re
from app.core.collections import COLLECTION_TELEFONES
from app.services.database import get_database

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Lista de Telefones - Motorista"])

@router.get("/motorista/{motorista}")
async def obter_telefone_motorista(motorista: str, base_name: str = Query('', alias='base_name')):
    """
    Busca telefone na coleção de telefones por nome do motorista e base (HUB).
    Regras estritas:
    - Motorista: match EXATO (case-insensitive), sem alterações no nome recebido
    - HUB: normalizado para "SIGLA -SC" e match EXATO
    """
    try:
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        collection = db[COLLECTION_TELEFONES]

        # Normalizar HUB/base para "SIGLA -SC"
        hub_upper = str(base_name or '').strip().upper()
        sigla_match = re.search(r'([A-Z]{2,4})', hub_upper)
        sigla = sigla_match.group(1) if sigla_match else ''
        hub_normalizado = f"{sigla} -SC" if sigla else hub_upper

        nome_original = str(motorista or '').strip()
        logger.info(f"[fone] STRICT buscar motorista='{nome_original}' base='{hub_normalizado}'")

        # Match EXATO (case-insensitive) de Motorista + HUB EXATO
        query = {
            'Motorista': { '$regex': f'^{re.escape(nome_original)}$', '$options': 'i' },
            'HUB': hub_normalizado
        }
        doc = await collection.find_one(query)
        if doc and doc.get('Contato'):
            logger.info(f"[fone] STRICT match encontrado para motorista='{nome_original}' hub='{hub_normalizado}'")
            return {
                'success': True,
                'tem_telefone': True,
                'match_exato': True,
                'telefone': doc.get('Contato'),
                'hub': hub_normalizado
            }

        return { 'success': True, 'tem_telefone': False, 'match_exato': False }
    except Exception as e:
        logger.error(f"Erro ao obter telefone do motorista: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {e}")


@router.post("/motorista/{motorista}/telefone")
async def adicionar_telefone_motorista(motorista: str, base_name: str = Query('', alias='base_name'), telefone: str = Query(...)):
    """
    Adiciona (ou atualiza) um telefone para um motorista e base (HUB) na coleção de telefones.
    Se já existir documento com mesmo Motorista+HUB, faz update do Contato.
    """
    try:
        if not telefone:
            raise HTTPException(status_code=400, detail="Telefone é obrigatório")

        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        collection = db[COLLECTION_TELEFONES]

        hub_upper = str(base_name).strip().upper()
        sigla_match = re.search(r'([A-Z]{2,4})', hub_upper)
        sigla = sigla_match.group(1) if sigla_match else ''
        hub_normalizado = f"{sigla} -SC" if sigla else hub_upper

        ts = datetime.now().isoformat()

        # Upsert por Motorista+HUB
        filtro = {
            'Motorista': { '$regex': f'^{re.escape(motorista)}$', '$options': 'i' },
            'HUB': hub_normalizado
        }

        update_doc = {
            '$set': {
                'Motorista': motorista,
                'HUB': hub_normalizado,
                'Contato': telefone,
                'timestamp': ts,
                'origem': 'overlay_add'
            },
            '$setOnInsert': {
                'Data': '',
                'Status': '',
                'Cidade': '',
                'Responsável pelo Cadastro': '',
                'TECHIS': '',
                'OA': '',
                'JMS': '',
                'Nº OA': '',
                'Observações': '',
                'Login': '',
                'Obs. Base': '',
                'Treinamento - Qualidade': ''
            }
        }

        result = await collection.update_one(filtro, update_doc, upsert=True)
        inserted = 1 if result.upserted_id else 0
        updated = result.modified_count if not result.upserted_id else 0

        return { 'success': True, 'inserted': inserted, 'updated': updated }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao adicionar/atualizar telefone: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {e}")

