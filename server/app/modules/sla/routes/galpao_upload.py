"""
Rotas de upload de entradas no galpão
"""
from fastapi import APIRouter, HTTPException, UploadFile, File
import tempfile
import os
from app.modules.sla.services.galpao_service import GalpaoService

router = APIRouter(prefix="/galpao", tags=["SLA - Galpão Upload"])

# Instância do serviço
galpao_service = GalpaoService()

@router.post("/upload/{base_name}")
async def upload_entradas_galpao(
    base_name: str,
    file: UploadFile = File(...)
):
    """
    Upload de arquivo Excel com entradas no galpão
    
    Args:
        base_name: Nome da base
        file: Arquivo Excel com entradas
        
    Returns:
        Resultado do processamento
    """
    try:
        # Validar tipo de arquivo
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400, 
                detail="Arquivo deve ser Excel (.xlsx ou .xls)"
            )
        
        # Salvar arquivo temporário
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
        
        try:
            # Processar arquivo
            result = await galpao_service.upload_entradas_excel(temp_file_path, base_name)
            
            if result.get("success"):
                return {
                    "success": True,
                    "message": result["message"],
                    "data": {
                        "total_entradas": result["total_entradas"],
                        "entradas_duplicadas": result.get("entradas_duplicadas", 0)
                    }
                }
            else:
                raise HTTPException(
                    status_code=400,
                    detail=result.get("error", "Erro no processamento")
                )
                
        finally:
            # Limpar arquivo temporário
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

