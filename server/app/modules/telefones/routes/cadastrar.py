"""
Rota para cadastro manual de motoristas
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import logging
import re
from datetime import datetime
from app.core.collections import COLLECTION_TELEFONES
from app.services.database import get_database

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Lista de Telefones - Cadastro"])

class CadastroMotoristaRequest(BaseModel):
    Data: str
    Motorista: str
    Status: str
    Cidade: str
    HUB: str
    Contato: str

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

@router.post("/cadastrar")
async def cadastrar_motorista(dados: CadastroMotoristaRequest):
    """
    Cadastra um novo motorista na coleção de telefones
    """
    try:
        # Validações
        if not dados.Motorista or not dados.Motorista.strip():
            raise HTTPException(status_code=400, detail="Campo Motorista é obrigatório")
        
        if not dados.Cidade or not dados.Cidade.strip():
            raise HTTPException(status_code=400, detail="Campo Cidade é obrigatório")
        
        if not dados.HUB or not dados.HUB.strip():
            raise HTTPException(status_code=400, detail="Campo HUB é obrigatório")
        
        if not dados.Contato or not dados.Contato.strip():
            raise HTTPException(status_code=400, detail="Campo Contato é obrigatório")
        
        if dados.Status not in ['Aprovado', 'Pendente', 'Rejeitado']:
            raise HTTPException(status_code=400, detail="Status deve ser: Aprovado, Pendente ou Rejeitado")
        
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não está conectado")
        
        collection = db[COLLECTION_TELEFONES]
        
        # Normalizar HUB
        hub_normalizado = normalizar_hub(dados.HUB)
        
        # Criar documento
        documento = {
            'Data': dados.Data,
            'Motorista': dados.Motorista.strip(),
            'Status': dados.Status,
            'Cidade': dados.Cidade.strip(),
            'HUB': hub_normalizado,
            'Contato': dados.Contato.strip(),
            'timestamp': datetime.now().isoformat(),
            'origem': 'cadastro_manual'
        }
        
        # Inserir na coleção
        result = await collection.insert_one(documento)
        
        return {
            'success': True,
            'message': 'Motorista cadastrado com sucesso',
            'id': str(result.inserted_id)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao cadastrar motorista: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


