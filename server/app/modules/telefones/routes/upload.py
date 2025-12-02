"""
Rota de upload de arquivos Excel para lista de telefones
"""
from fastapi import APIRouter, HTTPException, UploadFile, File
import logging
from app.services.excel_processor import ExcelProcessor
from app.core.collections import COLLECTION_TELEFONES
from datetime import datetime
from app.services.database import db

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Lista de Telefones - Upload"])

@router.post("/upload")
async def upload_excel_lista_telefones(file: UploadFile = File(...)):
    """
    Upload de Excel (lista telefônica) e salvamento como documentos planos na coleção de telefones.
    """
    try:
        if not file.filename.lower().endswith((".xlsx", ".xls")):
            raise HTTPException(status_code=400, detail="Arquivo deve ser Excel (.xlsx, .xls)")

        content = await file.read()
        processor = ExcelProcessor()
        data, columns_found = await processor.process_file(content, file.filename)

        if not data:
            raise HTTPException(status_code=400, detail="Nenhum dado válido encontrado no arquivo")

        # Padronizar HUB/base se aplicável
        data = processor.process_hub_column(data)

        # Inserir como documentos planos
        collection = db.database[COLLECTION_TELEFONES]
        ts = datetime.now().isoformat()
        docs = []
        for item in data:
            doc = dict(item)
            doc["timestamp"] = ts
            doc["origem"] = "upload_direto"
            docs.append(doc)

        if docs:
            await collection.insert_many(docs)

        return {
            "success": True,
            "inserted": len(docs),
            "filename": file.filename,
            "message": f"Arquivo importado com sucesso! {len(docs)} registros salvos em '{COLLECTION_TELEFONES}'.",
            "columns_found": columns_found
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro no upload: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {e}")

