"""
Rotas de cidades para cálculo SLA
"""
from fastapi import APIRouter, HTTPException
from app.modules.sla.services.sla_calculator import SLACalculator

router = APIRouter(tags=["SLA Calculator - Cities"])

# Instância do calculador
sla_calculator = SLACalculator()

@router.get("/cities/{base_name}")
async def get_base_cities(base_name: str):
    """
    Retorna lista de cidades disponíveis para uma base
    
    Args:
        base_name: Nome da base
        
    Returns:
        Lista de cidades
    """
    try:
        
        cities = await sla_calculator.get_available_cities(base_name)
        
        return {
            "success": True,
            "data": {
                "base": base_name,
                "cities": cities,
                "total": len(cities)
            },
            "message": f"Encontradas {len(cities)} cidades para a base '{base_name}'"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

