"""
Rotas de upload otimizadas para D-1 - grandes volumes (200k-400k registros)
"""
from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
import logging
from datetime import datetime
import time
import asyncio
from app.modules.d1.models.d1_model import UploadResponse
from app.services.database import (
    insert_d1_main,
    insert_d1_chunk,
    insert_d1_chunks_bulk,
    update_d1_status,
    get_database
)
from app.modules.d1.services.d1_processor import D1Processor
from app.core.collections import COLLECTION_D1_MAIN, COLLECTION_D1_CHUNKS

logger = logging.getLogger(__name__)

router = APIRouter(tags=["D-1 - Upload"])

# Configurações otimizadas para grandes volumes
CHUNK_SIZE = 5000  # Chunks maiores para reduzir número de operações
BULK_INSERT_SIZE = 10  # Inserir 10 chunks por vez em bulk

@router.post("/upload", response_model=UploadResponse)
async def upload_d1_excel(file: UploadFile = File(...)):
    """
    📤 UPLOAD DE ARQUIVO EXCEL D-1 - OTIMIZADO PARA GRANDES VOLUMES
    
    Processa arquivos Excel com 200k-400k registros de forma otimizada:
    - Processamento assíncrono
    - Chunks de 5000 registros
    - Inserção em bulk no MongoDB
    - Logs de progresso
    
    Args:
        file: Arquivo Excel (.xlsx, .xls)
        
    Returns:
        UploadResponse com informações do processamento
    """
    start_time = time.time()
    
    try:
        # Verificar tipo de arquivo
        if not file.filename.lower().endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400, 
                detail="Arquivo deve ser Excel (.xlsx ou .xls)"
            )
        
        logger.info(f"🚀 Iniciando upload D-1: {file.filename}")
        
        # Processar arquivo com processador otimizado
        processor = D1Processor(chunk_size=CHUNK_SIZE)
        file_content = await file.read()
        
        logger.info(f"📊 Processando arquivo {file.filename} (tamanho: {len(file_content):,} bytes)...")
        dados_processados, columns_found = await processor.process_file(file_content, file.filename)
        
        total_items = len(dados_processados)
        logger.info(f"✅ Arquivo processado: {total_items:,} registros, {len(columns_found)} colunas")
        
        # Log de amostra dos primeiros dados
        if total_items > 0:
            logger.info(f"📋 Amostra do primeiro registro: {list(dados_processados[0].keys())[:5]}...")
            logger.info(f"📋 Primeiro registro (amostra): {dict(list(dados_processados[0].items())[:3])}")
        else:
            logger.warning(f"⚠️ ATENÇÃO: Nenhum registro foi processado do arquivo {file.filename}")
            raise HTTPException(
                status_code=400,
                detail="Nenhum dado foi processado do arquivo. Verifique se o arquivo contém dados além dos cabeçalhos."
            )
        
        # Calcular total de chunks
        total_chunks = (total_items + CHUNK_SIZE - 1) // CHUNK_SIZE
        logger.info(f"📊 Total de chunks a serem criados: {total_chunks}")
        
        # Criar documento principal
        main_document = {
            "filename": file.filename,
            "upload_date": datetime.now(),
            "total_items": total_items,
            "chunk_size": CHUNK_SIZE,
            "total_chunks": total_chunks,
            "columns_found": columns_found,
            "status": "processing",
            "processing_time": None
        }
        
        # Salvar documento principal
        main_id = await insert_d1_main(main_document)
        logger.info(f"📄 Documento principal criado: {main_id}")
        
        # Processar e salvar chunks de forma otimizada
        logger.info(f"📦 Iniciando salvamento de {total_chunks} chunks com {total_items:,} registros...")
        chunks_saved = await _save_chunks_optimized(main_id, dados_processados, processor)
        
        logger.info(f"📦 Salvamento concluído: {chunks_saved} chunks salvos de {total_chunks} esperados")
        
        # Calcular tempo de processamento
        processing_time = time.time() - start_time
        
        # Atualizar status para concluído
        await update_d1_status(main_id, "completed", processing_time=processing_time)
        
        logger.info(
            f"🎉 Upload D-1 concluído!\n"
            f"   Arquivo: {file.filename}\n"
            f"   Registros: {total_items:,}\n"
            f"   Chunks: {chunks_saved}\n"
            f"   Tempo: {processing_time:.2f}s"
        )
        
        return UploadResponse(
            success=True,
            message=f"Arquivo {file.filename} processado com sucesso! {total_items:,} registros salvos em {chunks_saved} chunks.",
            id=main_id,
            filename=file.filename,
            total_items=total_items,
            total_chunks=chunks_saved,
            processing_time=processing_time
        )
        
    except HTTPException:
        raise
    except Exception as e:
        processing_time = time.time() - start_time
        error_msg = f"Erro ao processar arquivo: {str(e)}"
        logger.error(f"❌ {error_msg}")
        
        # Se o documento principal foi criado, atualizar status de erro
        if 'main_id' in locals():
            try:
                await update_d1_status(main_id, "error", error_message=error_msg, processing_time=processing_time)
            except:
                pass
        
        raise HTTPException(
            status_code=500,
            detail=error_msg
        )


async def _save_chunks_optimized(
    main_id: str,
    dados: list,
    processor: D1Processor
) -> int:
    """
    Salva chunks de forma otimizada usando bulk insert
    
    Args:
        main_id: ID do documento principal
        dados: Lista de dados processados
        processor: Instância do processador
        
    Returns:
        Número de chunks salvos
    """
    # Dividir em chunks
    chunks_data = processor.create_chunks(dados)
    total_chunks = len(chunks_data)
    
    logger.info(f"📦 Preparando {total_chunks} chunks para inserção...")
    logger.info(f"   Tamanho de cada chunk: até {CHUNK_SIZE} registros")
    logger.info(f"   Bulk insert size: {BULK_INSERT_SIZE} chunks por vez")
    
    # Preparar documentos de chunks
    chunk_documents = []
    chunks_saved = 0
    
    for i, chunk_data in enumerate(chunks_data, start=1):
        chunk_document = {
            "main_document_id": main_id,
            "chunk_number": i,
            "chunk_data": chunk_data,
            "chunk_size": len(chunk_data),
            "upload_date": datetime.now()
        }
        chunk_documents.append(chunk_document)
        
        # Log do primeiro chunk para debug
        if i == 1:
            logger.info(f"📋 Primeiro chunk preparado: {len(chunk_data)} registros, {len(chunk_data[0]) if chunk_data else 0} colunas")
        
        # Inserir em bulk quando atingir o tamanho do bulk
        if len(chunk_documents) >= BULK_INSERT_SIZE:
            try:
                inserted_count = await insert_d1_chunks_bulk(chunk_documents)
                chunks_saved += inserted_count
                logger.info(f"✅ Chunks salvos em bulk: {inserted_count} chunks (Total: {chunks_saved}/{total_chunks} - {chunks_saved*100//total_chunks if total_chunks > 0 else 0}%)")
                chunk_documents = []  # Limpar lista
            except Exception as e:
                logger.error(f"❌ Erro ao inserir chunks em bulk: {e}")
                logger.exception(e)
                # Fallback: inserir um por um
                for doc in chunk_documents:
                    try:
                        await insert_d1_chunk(doc)
                        chunks_saved += 1
                        logger.debug(f"✅ Chunk {doc['chunk_number']} salvo individualmente")
                    except Exception as e2:
                        logger.error(f"❌ Erro ao inserir chunk {doc.get('chunk_number', '?')} individual: {e2}")
                chunk_documents = []
    
    # Inserir chunks restantes
    if chunk_documents:
        logger.info(f"📦 Inserindo {len(chunk_documents)} chunks restantes...")
        try:
            inserted_count = await insert_d1_chunks_bulk(chunk_documents)
            chunks_saved += inserted_count
            logger.info(f"✅ Chunks finais salvos em bulk: {inserted_count} chunks")
        except Exception as e:
            logger.error(f"❌ Erro ao inserir chunks finais: {e}")
            logger.exception(e)
            # Fallback: inserir um por um
            for doc in chunk_documents:
                try:
                    await insert_d1_chunk(doc)
                    chunks_saved += 1
                    logger.debug(f"✅ Chunk final {doc['chunk_number']} salvo individualmente")
                except Exception as e2:
                    logger.error(f"❌ Erro ao inserir chunk final {doc.get('chunk_number', '?')}: {e2}")
    
    logger.info(f"✅ Total de chunks salvos: {chunks_saved}/{total_chunks}")
    
    if chunks_saved != total_chunks:
        logger.warning(f"⚠️ ATENÇÃO: Esperado {total_chunks} chunks, mas apenas {chunks_saved} foram salvos!")
    
    return chunks_saved


@router.delete("/clear-all")
async def clear_all_d1_data():
    """
    Deleta todos os dados das coleções d1_main e d1_chunks (Gestão de 1 Mês)
    """
    try:
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Não foi possível conectar ao banco de dados")
        
        # Contar antes da exclusão
        count_main_before = await db[COLLECTION_D1_MAIN].count_documents({})
        count_chunks_before = await db[COLLECTION_D1_CHUNKS].count_documents({})
        
        # Deletar todos os documentos
        result_main = await db[COLLECTION_D1_MAIN].delete_many({})
        result_chunks = await db[COLLECTION_D1_CHUNKS].delete_many({})
        
        total_deleted = result_main.deleted_count + result_chunks.deleted_count
        
        logger.info(
            f"🗑️ Dados D-1 deletados:\n"
            f"   d1_main: {result_main.deleted_count} documentos\n"
            f"   d1_chunks: {result_chunks.deleted_count} documentos\n"
            f"   Total: {total_deleted} documentos"
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Todos os dados de Gestão de 1 Mês foram deletados com sucesso",
                "deleted_count": total_deleted,
                "deleted_counts": {
                    "d1_main": result_main.deleted_count,
                    "d1_chunks": result_chunks.deleted_count,
                    "total": total_deleted
                },
                "previous_counts": {
                    "d1_main": count_main_before,
                    "d1_chunks": count_chunks_before,
                    "total": count_main_before + count_chunks_before
                }
            }
        )
        
    except Exception as e:
        logger.error(f"Erro ao deletar dados D-1: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao deletar dados D-1: {str(e)}"
        )

