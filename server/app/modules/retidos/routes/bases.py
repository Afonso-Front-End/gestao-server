"""
Rotas para listar bases
"""
from fastapi import APIRouter, HTTPException
import logging
from app.core.collections import COLLECTION_PEDIDOS_RETIDOS_TABELA
from app.services.database import db, get_all_pedidos_retidos, get_pedidos_retidos_chunks

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pedidos Retidos - Bases"])

@router.get("/bases-tabela-dados")
async def get_bases_tabela_dados():
    """
    🏢 LISTA TODAS AS BASES DE ENTREGA (tabela_dados)
    Lê de 'tabela_dados' (documento principal) o campo 'bases_entrega' dos uploads completed
    """
    try:
        collection = db.database[COLLECTION_PEDIDOS_RETIDOS_TABELA]
        cursor = collection.find({"status": "completed"})
        bases_unicas = set()
        async for doc in cursor:
            for base in doc.get("bases_entrega", []) or []:
                base_str = str(base).strip()
                if base_str:
                    bases_unicas.add(base_str)
        bases_lista = sorted(list(bases_unicas))
        return {"success": True, "data": bases_lista, "total": len(bases_lista)}
    except Exception as e:
        logger.error(f"Erro ao buscar bases (tabela_dados): {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/bases")
async def get_all_bases():
    """
    🏢 LISTA TODAS AS BASES ENCONTRADAS
    Retorna todas as bases únicas encontradas nos arquivos de monitoramento
    """
    try:
        # Buscar todos os documentos principais
        main_docs = await get_all_pedidos_retidos()
        
        if not main_docs:
            return {"data": [], "message": "Nenhuma base encontrada"}
        
        # Coletar todas as bases únicas
        todas_bases = set()
        
        for main_doc in main_docs:
            # Verificar se é um documento com chunks
            if main_doc.get("status") == "completed" and main_doc.get("total_chunks", 0) > 0:
                # Buscar chunks do documento
                chunks = await get_pedidos_retidos_chunks(main_doc["id"])
                
                for chunk in chunks:
                    if chunk.get("chunk_data"):
                        for item in chunk["chunk_data"]:
                            # Buscar base na coluna "Unidade responsável" ou "BASE"
                            base = item.get("Base de entrega", "").strip()
                            if not base:
                                base = item.get("BASE", "").strip()
                            if base:
                                todas_bases.add(base)
            
            # Compatibilidade com documentos antigos (sem chunks)
            elif main_doc.get("data"):
                for item in main_doc["data"]:
                    # Buscar base na coluna "Unidade responsável" ou "BASE"
                    base = item.get("Base de entrega", "").strip()
                    if not base:
                        base = item.get("BASE", "").strip()
                    if base:
                        todas_bases.add(base)
            
            # Adicionar bases do documento principal se existirem
            if main_doc.get("bases"):
                for base in main_doc["bases"]:
                    if base and base.strip():
                        todas_bases.add(base.strip())
        
        # Converter para lista ordenada
        bases_lista = sorted(list(todas_bases))
        
        logger.info(f"🏢 Total de bases únicas encontradas: {len(bases_lista)}")
        
        return {
            "data": bases_lista,
            "total_bases": len(bases_lista),
            "message": f"Encontradas {len(bases_lista)} bases únicas"
        }
    except Exception as e:
        logger.error(f"Erro ao buscar bases: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

