"""
Rotas de processamento de bases SLA
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from typing import Dict, Any
from app.modules.sla.services.sla_bases_service import SLABasesService

router = APIRouter(tags=["SLA Bases - Process"])

# Instância do serviço
sla_bases_service = SLABasesService()

@router.post("/process")
async def process_selected_bases(request: Dict[str, Any]) -> JSONResponse:
    """
    Processa as bases selecionadas e salva os dados
    
    Args:
        request: Dict com lista de bases selecionadas
        
    Returns:                        
        JSONResponse com resultado do processamento
    """
    try:
        selected_bases = request.get("bases", [])
        
        if not selected_bases:
            raise HTTPException(        
                status_code=400,
                detail="Nenhuma base foi selecionada"
            )
        
        
        # Processar bases
        result = await sla_bases_service.process_selected_bases(selected_bases)
        
        if result["success"]:
            return JSONResponse(
                status_code=200,
                content={
                    "message": "Bases processadas com sucesso",
                    "data": result
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

