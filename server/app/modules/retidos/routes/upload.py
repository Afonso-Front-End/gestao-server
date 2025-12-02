"""
Rotas de upload de arquivos Excel
"""
from fastapi import APIRouter, HTTPException, UploadFile, File
import logging
from datetime import datetime
from app.modules.retidos.models.pedidos_retidos import UploadResponse
from app.services.database import (
    insert_pedidos_retidos,
    insert_pedidos_retidos_chunk,
    update_pedidos_retidos_status,
    insert_tabela_dados,
    insert_tabela_dados_chunk,
    update_tabela_dados_status,
    clear_tabela_dados_collections,
)
from app.modules.retidos.services.excel_processor import ExcelProcessor

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pedidos Retidos - Upload"])

@router.post("/upload", response_model=UploadResponse)
async def upload_excel(file: UploadFile = File(...)):
    """
    📤 UPLOAD DE ARQUIVO EXCEL COM CHUNKS
    Recebe um arquivo Excel (.xlsx, .xls) contendo dados de monitoramento,
    processa e normaliza os dados, salva no MongoDB em chunks e retorna um ID único.
    """
    try:
        # Verificar tipo de arquivo
        if not file.filename.lower().endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400, detail="Arquivo deve ser Excel (.xlsx ou .xls)"
            )
        
        # Processar arquivo
        processor = ExcelProcessor()
        file_content = await file.read()
        dados_processados, columns_found = await processor.process_file(file_content, file.filename)
        
        # Extrair bases únicas da coluna "Unidade responsável"
        bases_unicas = set()
        for item in dados_processados:
            unidade = item.get("Unidade responsável", "").strip()
            if unidade:
                bases_unicas.add(unidade)
        
        logger.info(f"🏢 Bases encontradas no arquivo: {len(bases_unicas)} - {list(bases_unicas)}")
        
        # Configurações de chunk
        CHUNK_SIZE = 1000  # Processar 1000 registros por vez
        total_items = len(dados_processados)
        
        logger.info(f"📊 Processando arquivo {file.filename} com {total_items} registros em chunks de {CHUNK_SIZE}")
        
        # Criar documento principal
        main_document = {
            "filename": file.filename,
            "upload_date": datetime.now(),
            "total_items": total_items,
            "chunk_size": CHUNK_SIZE,
            "total_chunks": (total_items + CHUNK_SIZE - 1) // CHUNK_SIZE,  # Arredondar para cima
            "columns_found": columns_found,
            "bases": list(bases_unicas),
            "total_bases": len(bases_unicas),
            "status": "processing"
        }
        
        # Salvar documento principal primeiro
        main_id = await insert_pedidos_retidos(main_document)
        logger.info(f"✅ Documento principal criado com ID: {main_id}")
        
        # Processar dados em chunks
        chunks_saved = 0
        for i in range(0, total_items, CHUNK_SIZE):
            chunk_data = dados_processados[i:i + CHUNK_SIZE]
            chunk_number = (i // CHUNK_SIZE) + 1
            
            # Criar documento do chunk
            chunk_document = {
                "main_document_id": main_id,
                "chunk_number": chunk_number,
                "chunk_data": chunk_data,
                "chunk_size": len(chunk_data),
                "upload_date": datetime.now()
            }
            
            # Salvar chunk
            await insert_pedidos_retidos_chunk(chunk_document)
            chunks_saved += 1
            
            logger.info(f"📦 Chunk {chunk_number} salvo com {len(chunk_data)} registros")
        
        # Atualizar status do documento principal
        await update_pedidos_retidos_status(main_id, "completed")
        
        logger.info(f"🎉 Upload concluído! {chunks_saved} chunks salvos para {total_items} registros")
        
        return UploadResponse(
            success=True,
            message=f"Arquivo {file.filename} processado com sucesso! {total_items} registros salvos em {chunks_saved} chunks.",
            id=str(main_id),
            filename=file.filename,
            total_items=total_items,
        )
    except Exception as e:
        logger.error(f"❌ Erro no upload: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Erro ao processar arquivo: {str(e)}"
        )

@router.post("/upload-tabela-dados", response_model=UploadResponse)
async def upload_tabela_dados(file: UploadFile = File(...)):
    """
    📤 UPLOAD DE ARQUIVO EXCEL - TABELA DE DADOS
    Recebe um arquivo Excel (.xlsx, .xls) contendo dados de tabela,
    processa e normaliza os dados, salva no MongoDB em chunks e retorna um ID único.
    """
    try:
        # Verificar tipo de arquivo
        if not file.filename.lower().endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400, detail="Arquivo deve ser Excel (.xlsx ou .xls)"
            )
        
        # 🗑️ LIMPAR coleções antigas ANTES de processar o novo arquivo
        logger.info(f"🗑️ Limpando coleções antigas de tabela de dados...")
        clear_result = await clear_tabela_dados_collections()
        logger.info(f"✅ Limpeza concluída: {clear_result['main_deleted']} docs principais e {clear_result['chunks_deleted']} chunks removidos")
        
        # Processar arquivo
        processor = ExcelProcessor()
        file_content = await file.read()
        dados_processados, columns_found = await processor.process_file(file_content, file.filename)
        
        # Extrair bases únicas da coluna "Base de entrega"
        bases_unicas = set()
        for item in dados_processados:
            base_entrega = item.get("Base de entrega", "").strip()
            if base_entrega:
                bases_unicas.add(base_entrega)
        
        logger.info(f"📊 Arquivo de tabela de dados processado: {file.filename}")
        logger.info(f"📋 Colunas encontradas: {columns_found}")
        logger.info(f"🏢 Bases de entrega encontradas: {len(bases_unicas)} - {list(bases_unicas)}")
        
        # Configurações de chunk
        CHUNK_SIZE = 1000  # Processar 1000 registros por vez
        total_items = len(dados_processados)
        
        logger.info(f"📊 Processando arquivo {file.filename} com {total_items} registros em chunks de {CHUNK_SIZE}")
        
        # Criar documento principal
        main_document = {
            "filename": file.filename,
            "upload_date": datetime.now(),
            "total_items": total_items,
            "chunk_size": CHUNK_SIZE,
            "total_chunks": (total_items + CHUNK_SIZE - 1) // CHUNK_SIZE,
            "columns_found": columns_found,
            "bases_entrega": list(bases_unicas),
            "total_bases": len(bases_unicas),
            "status": "processing",
            "file_type": "tabela_dados"
        }
        
        # Salvar documento principal
        main_id = await insert_tabela_dados(main_document)
        logger.info(f"📄 Documento principal criado com ID: {main_id}")
        
        # Processar em chunks
        chunks_saved = 0
        for i in range(0, total_items, CHUNK_SIZE):
            chunk_data = dados_processados[i:i + CHUNK_SIZE]
            chunk_number = (i // CHUNK_SIZE) + 1
            
            chunk_document = {
                "main_id": main_id,
                "chunk_number": chunk_number,
                "data": chunk_data,
                "items_count": len(chunk_data)
            }
            
            await insert_tabela_dados_chunk(chunk_document)
            chunks_saved += 1
            
            if chunks_saved % 10 == 0:  # Log a cada 10 chunks
                logger.info(f"📦 Chunks salvos: {chunks_saved}")
        
        # Atualizar status para concluído
        await update_tabela_dados_status(main_id, "completed")
        
        logger.info(f"✅ Upload concluído! {total_items} registros salvos em {chunks_saved} chunks.")
        
        return UploadResponse(
            success=True,
            message=f"Arquivo {file.filename} processado com sucesso! {total_items} registros salvos em {chunks_saved} chunks.",
            id=str(main_id),
            filename=file.filename,
            total_items=total_items,
        )
    except Exception as e:
        logger.error(f"❌ Erro no upload da tabela de dados: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Erro ao processar arquivo: {str(e)}"
        )

