"""
Rotas para listar opções de filtros (tipos, aging, cidades)
"""
from fastapi import APIRouter, HTTPException
import logging
import re
from app.core.collections import COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS
from app.services.database import db, get_all_pedidos_retidos, get_pedidos_retidos_chunks

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pedidos Retidos - Selects"])

@router.get("/tipos-operacao")
async def get_all_tipos_operacao():
    """
    🔧 LISTA TODOS OS TIPOS DE OPERAÇÃO
    Retorna todos os tipos de operação únicos encontrados nos arquivos
    """
    try:
        # Buscar todos os documentos principais
        main_docs = await get_all_pedidos_retidos()
        
        if not main_docs:
            return {"data": [], "message": "Nenhum tipo de operação encontrado"}
        
        # Coletar todos os tipos únicos
        todos_tipos = set()
        
        for main_doc in main_docs:
            # Verificar se é um documento com chunks
            if main_doc.get("status") == "completed" and main_doc.get("total_chunks", 0) > 0:
                # Buscar chunks do documento
                chunks = await get_pedidos_retidos_chunks(main_doc["id"])
                
                for chunk in chunks:
                    if chunk.get("chunk_data"):
                        for item in chunk["chunk_data"]:
                            # Buscar tipo de operação em várias colunas possíveis
                            tipo = (item.get("Tipo da última operação", "")).strip()
                            
                            if tipo:
                                todos_tipos.add(tipo)
            
            # Compatibilidade com documentos antigos (sem chunks)
            elif main_doc.get("data"):
                for item in main_doc["data"]:
                    # Buscar tipo de operação em várias colunas possíveis
                    tipo = (item.get("Tipo da última operação", "")).strip()
                    
                    if tipo:
                        todos_tipos.add(tipo)
        
        # Converter para lista ordenada
        tipos_lista = sorted(list(todos_tipos))
        
        logger.info(f"🔧 Total de tipos de operação únicos encontrados: {len(tipos_lista)}")

        return {
            "data": tipos_lista,
            "total_tipos": len(tipos_lista),
            "message": f"Encontrados {len(tipos_lista)} tipos de operação únicos"
        }
    except Exception as e:
        logger.error(f"Erro ao buscar tipos de operação: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/aging")
async def get_all_aging():
    """
    ⏰ LISTA TODOS OS AGING
    Retorna todos os aging únicos encontrados nos arquivos, ordenados do menor para o maior
    """
    try:
        # Buscar todos os documentos principais
        main_docs = await get_all_pedidos_retidos()
        
        if not main_docs:
            return {"data": [], "message": "Nenhum aging encontrado"}
        
        # Coletar todos os aging únicos
        todos_aging = set()
        
        for main_doc in main_docs:
            # Verificar se é um documento com chunks
            if main_doc.get("status") == "completed" and main_doc.get("total_chunks", 0) > 0:
                # Buscar chunks do documento
                chunks = await get_pedidos_retidos_chunks(main_doc["id"])
                
                for chunk in chunks:
                    if chunk.get("chunk_data"):
                        for item in chunk["chunk_data"]:
                            # Buscar aging em várias colunas possíveis
                            aging = (item.get("Aging", "") or
                                   item.get("AGING", "") or
                                   item.get("Aging (dias)", "") or
                                   item.get("Aging dias", "") or
                                   item.get("Dias Aging", "") or
                                   item.get("Tempo Aging", "") or
                                   item.get("Idade", "")).strip()
                            
                            if aging:
                                todos_aging.add(aging)
            
            # Compatibilidade com documentos antigos (sem chunks)
            elif main_doc.get("data"):
                for item in main_doc["data"]:
                    # Buscar aging em várias colunas possíveis
                    aging = (item.get("Aging", "") or
                           item.get("AGING", "") or
                           item.get("Aging (dias)", "") or
                           item.get("Aging dias", "") or
                           item.get("Dias Aging", "") or
                           item.get("Tempo Aging", "") or
                           item.get("Idade", "")).strip()
                    
                    if aging:
                        todos_aging.add(aging)
        
        # Ordenar por número de dias (menor para maior)
        def extract_number(aging_str):
            """Extrai número do aging para ordenação"""
            match = re.search(r'(\d+)', str(aging_str))
            return int(match.group(1)) if match else 0
        
        aging_lista = sorted(list(todos_aging), key=lambda x: extract_number(x))
        
        logger.info(f"⏰ Total de aging únicos encontrados: {len(aging_lista)}")
        
        return {
            "data": aging_lista,
            "total_aging": len(aging_lista),
            "message": f"Encontrados {len(aging_lista)} aging únicos"
        }
    except Exception as e:
        logger.error(f"Erro ao buscar aging: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/cidades")
async def get_all_cidades(bases: str | None = None):
    """
    🏙️ Lista de cidades únicas a partir de tabela_dados_chunks, opcionalmente filtradas por 'bases'.
    """
    try:
        bases_list = [b.strip() for b in bases.split(',')] if bases else []
        collection = db.database[COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS]
        total_chunks = await collection.count_documents({})
        if total_chunks == 0:
            return {"success": True, "data": [], "total": 0}
        cidades = set()
        cursor = collection.find({}).sort("chunk_number", 1)
        async for chunk in cursor:
            for item in chunk.get("data", []) or []:
                base = (item.get("Base de entrega", "").strip() or item.get("BASE", "").strip())
                if bases_list and base not in bases_list:
                    continue
                cidade = (
                    item.get("Cidade Destino")
                    or item.get("Cidade destino")
                    or item.get("Cidade")
                    or ""
                )
                cidade = str(cidade).strip()
                if cidade:
                    cidades.add(cidade)
        lista = sorted(list(cidades))
        return {"success": True, "data": lista, "total": len(lista)}
    except Exception as e:
        logger.error(f"Erro ao buscar cidades (tabela_dados_chunks): {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

