"""
Rotas de upload de arquivos SLA
"""
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from app.modules.sla.services.sla_processor import SLAProcessor

router = APIRouter(tags=["SLA - Upload"])

# Instância do processador
sla_processor = SLAProcessor()

@router.post("/upload")
async def upload_sla_file(file: UploadFile = File(...)) -> JSONResponse:
    """
    Endpoint para upload e processamento de arquivos SLA
    
    Args:
        file: Arquivo Excel/CSV com dados SLA
        
    Returns:
        JSONResponse com resultado do processamento
    """
    try:
        # Validar se arquivo foi enviado
        if not file or not file.filename:
            raise HTTPException(
                status_code=400,
                detail="Nenhum arquivo foi enviado"
            )
        
        # Validar tipo de arquivo
        if not file.filename.lower().endswith(('.xlsx', '.xls', '.csv')):
            raise HTTPException(
                status_code=400,
                detail="Formato de arquivo não suportado. Use .xlsx, .xls ou .csv"
            )
        
        # Ler conteúdo do arquivo
        file_content = await file.read()
        
        if len(file_content) == 0:
            raise HTTPException(
                status_code=400,
                detail="Arquivo vazio"
            )
        
        # Processar arquivo
        result = await sla_processor.process_file(file_content, file.filename)
        
        if result["success"]:
            return JSONResponse(
                status_code=200,
                content={
                    "message": "Arquivo processado com sucesso",
                    "data": {
                        "file_id": result["file_id"],
                        "filename": file.filename,
                        "total_records": result["total_records"],
                        "total_chunks": result["total_chunks"]
                    }
                }
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=result["error"]
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno do servidor: {str(e)}"
        )

@router.post("/test-upload")
async def test_upload(file: UploadFile = File(...)) -> JSONResponse:
    """
    Endpoint de teste para upload simples
    """
    try:
        file_content = await file.read()
        return JSONResponse(
            status_code=200,
            content={
                "message": "Teste de upload bem-sucedido",
                "filename": file.filename,
                "size": len(file_content),
                "content_type": file.content_type
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={
                "error": str(e),
                "message": "Erro no teste de upload"
            }
        )

