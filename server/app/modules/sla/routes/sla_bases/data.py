"""
Rotas de consulta de dados de bases SLA
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.modules.sla.services.sla_bases_service import SLABasesService

router = APIRouter(tags=["SLA Bases - Data"])

# Instância do serviço
sla_bases_service = SLABasesService()

@router.get("/data/{base_name}")
async def get_base_data(base_name: str, limit: int = 100, skip: int = 0) -> JSONResponse:
    """
    Obtém os dados de uma base específica
    
    Args:
        base_name: Nome da base
        limit: Limite de registros (padrão: 100)
        skip: Registros para pular (padrão: 0)
        
    Returns:
        JSONResponse com dados da base
    """
    try:
        db = sla_bases_service._get_database()
        
        base_doc = await db.sla_bases_data.find_one({"base_name": base_name})
        
        if not base_doc:
            raise HTTPException(
                status_code=404,
                detail="Base não encontrada"
            )
        
        # Paginar dados
        data = base_doc.get("data", [])
        paginated_data = data[skip:skip + limit]
        
        # Limpar valores NaN dos dados
        def clean_data(records):
            cleaned_records = []
            for record in records:
                cleaned_record = {}
                for key, value in record.items():
                    if isinstance(value, float) and (value != value):  # Check for NaN
                        cleaned_record[key] = None
                    else:
                        cleaned_record[key] = value
                cleaned_records.append(cleaned_record)
            return cleaned_records
        
        cleaned_data = clean_data(paginated_data)
        
        return JSONResponse(
            status_code=200,
            content={
                "message": "Dados da base obtidos com sucesso",
                "data": {
                    "base_name": base_name,
                    "total_records": len(data),
                    "returned_records": len(cleaned_data),
                    "skip": skip,
                    "limit": limit,
                    "records": cleaned_data
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno do servidor: {str(e)}"
        )

