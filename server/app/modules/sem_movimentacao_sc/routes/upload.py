"""
Rotas para upload de dados de Sem Movimentação SC
"""
from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
import logging
from datetime import datetime
from app.modules.sem_movimentacao_sc.services.processor import SemMovimentacaoSCProcessor
from app.services.database import get_database
from app.core.collections import COLLECTION_SEM_MOVIMENTACAO_SC, COLLECTION_SEM_MOVIMENTACAO_SC_CHUNKS
from bson import ObjectId

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Sem Movimentação SC - Upload"])

processor = SemMovimentacaoSCProcessor()


@router.post("/upload")
async def upload_sem_movimentacao_sc(file: UploadFile = File(...)):
    """
    Upload de arquivo Excel com dados de Sem Movimentação SC
    
    Processa e salva os dados na coleção do MongoDB.
    As colunas são mapeadas automaticamente mesmo que estejam em ordem diferente.
    
    Colunas esperadas (serão mapeadas automaticamente):
    - Remessa
    - Nome da base mais recente
    - Unidade responsável
    - Base de entrega
    - Horário da última operação
    - Tipo da última operação
    - Operador do bipe mais recente
    - Aging
    - Número do ID
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
        
        logger.info(f"📤 Iniciando upload de {file.filename}")
        
        # Processar arquivo
        result = await processor.process_file(file_content, file.filename)
        
        if not result.get("success"):
            raise HTTPException(
                status_code=400,
                detail=f"Erro ao processar arquivo: {result.get('error', 'Erro desconhecido')}"
            )
        
        dados_processados = result.get("data", [])
        
        if len(dados_processados) == 0:
            raise HTTPException(
                status_code=400,
                detail="Nenhum registro válido foi encontrado no arquivo"
            )
        
        db = get_database()
        
        # Criar documento principal (metadados do arquivo)
        main_collection = db[COLLECTION_SEM_MOVIMENTACAO_SC]
        main_document = {
            "filename": file.filename,
            "upload_date": datetime.now(),
            "total_items": len(dados_processados),
            "columns_found": result.get("columns_found", []),
            "columns_mapped": result.get("columns_mapped", {}),
            "status": "processing"
        }
        
        main_result = await main_collection.insert_one(main_document)
        file_id = str(main_result.inserted_id)
        
        logger.info(f"📄 Documento principal criado: {file_id}")
        
        # Configuração de chunks para grandes volumes
        CHUNK_SIZE = 1000  # 1000 registros por chunk
        total_chunks = (len(dados_processados) + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        logger.info(f"💾 Salvando {len(dados_processados)} registros em {total_chunks} chunks...")
        
        # Salvar dados em chunks
        chunks_collection = db[COLLECTION_SEM_MOVIMENTACAO_SC_CHUNKS]
        chunks_to_insert = []
        chunks_saved = 0
        BULK_INSERT_SIZE = 10  # Inserir 10 chunks por vez
        
        for chunk_idx in range(0, len(dados_processados), CHUNK_SIZE):
            chunk_data = dados_processados[chunk_idx:chunk_idx + CHUNK_SIZE]
            chunk_number = (chunk_idx // CHUNK_SIZE)  # 0-based index
            
            # Criar documento do chunk
            chunk_document = {
                "id": None,  # Campo id (pode ser null)
                "chunk_index": chunk_number,
                "total_chunks": total_chunks,
                "file_id": file_id,
                "data": chunk_data,
                "created_at": datetime.now(),
                "status": "completed"
            }
            
            chunks_to_insert.append(chunk_document)
            
            # Inserir em bulk quando atingir BULK_INSERT_SIZE chunks ou for o último chunk
            if len(chunks_to_insert) >= BULK_INSERT_SIZE or chunk_number == total_chunks - 1:
                if chunks_to_insert:
                    result_insert = await chunks_collection.insert_many(chunks_to_insert, ordered=False)
                    inserted_count = len(result_insert.inserted_ids)
                    chunks_saved += inserted_count
                    # Calcular range de chunks (chunk_index é 0-based, mas log mostra 1-based)
                    first_chunk_idx = chunks_to_insert[0]['chunk_index']
                    last_chunk_idx = chunks_to_insert[-1]['chunk_index']
                    total_records = sum(len(c['data']) for c in chunks_to_insert)
                    logger.info(f"💾 Chunks {first_chunk_idx + 1}-{last_chunk_idx + 1}/{total_chunks} salvos ({inserted_count} chunks, {total_records} registros)")
                    chunks_to_insert = []
        
        # Atualizar documento principal com status concluído
        await main_collection.update_one(
            {"_id": main_result.inserted_id},
            {
                "$set": {
                    "status": "completed",
                    "total_chunks": chunks_saved,
                    "completed_at": datetime.now()
                }
            }
        )
        
        logger.info(f"✅ Upload concluído:")
        logger.info(f"   Registros processados: {len(dados_processados)}")
        logger.info(f"   Chunks salvos: {chunks_saved}/{total_chunks}")
        logger.info(f"   File ID: {file_id}")
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": f"Arquivo processado com sucesso",
                "filename": file.filename,
                "file_id": file_id,
                "total_rows": result.get("total_rows", 0),
                "total_valid": len(dados_processados),
                "total_empty": result.get("total_empty", 0),
                "total_chunks": chunks_saved,
                "columns_mapped": result.get("columns_mapped", {}),
                "processed_at": datetime.now().isoformat()
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao fazer upload de Sem Movimentação SC: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

