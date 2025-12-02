"""
Rotas de cálculo de métricas SLA
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from app.modules.sla.services.sla_calculator import SLACalculator

router = APIRouter(tags=["SLA Calculator - Metrics"])

# Instância do calculador
sla_calculator = SLACalculator()

@router.get("/metrics/{base_name}")
async def calculate_sla_metrics(
    base_name: str,
    cities: Optional[List[str]] = Query(None, description="Lista de cidades para filtrar")
):
    """
    Calcula métricas de SLA para uma base específica
    
    Args:
        base_name: Nome da base
        cities: Lista opcional de cidades para filtrar (pode ser passado como cities=city1&cities=city2 ou cities=city1)
        
    Returns:
        Métricas de SLA calculadas
    """
    try:
        # Normalizar cities: FastAPI pode passar como string única ou lista
        # Garantir que sempre seja uma lista ou None
        if cities is not None:
            if isinstance(cities, str):
                cities = [cities]
            elif not isinstance(cities, list):
                cities = list(cities) if cities else None
        
        result = await sla_calculator.calculate_sla_metrics(base_name, cities)
        
        # Verificar se há erro no resultado
        if not result.get("success", True) or "error" in result:
            # Retornar 200 com dados vazios em vez de 404
            error_msg = result.get("error", "Nenhum registro encontrado para a base especificada")
            return {
                "success": False,
                "data": {
                    "motoristas": [],
                    "total_pedidos": 0,
                    "total_entregues": 0,
                    "total_nao_entregues": 0,
                    "sla_percentual": 0.0
                },
                "error": error_msg,
                "message": error_msg
            }
        
        # Verificar se há dados válidos
        if "motoristas" not in result or not isinstance(result["motoristas"], list):
            # Retornar 200 com dados vazios em vez de 404
            return {
                "success": False,
                "data": {
                    "motoristas": [],
                    "total_pedidos": 0,
                    "total_entregues": 0,
                    "total_nao_entregues": 0,
                    "sla_percentual": 0.0
                },
                "error": f"Nenhum dado de motoristas encontrado para a base: {base_name}",
                "message": f"Nenhum dado de motoristas encontrado para a base: {base_name}"
            }
        
        return {
            "success": True,
            "data": result,
            "message": f"SLA calculado com sucesso para {len(result['motoristas'])} motoristas"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Erro ao calcular métricas SLA para base '{base_name}': {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500, 
            detail=f"Erro interno ao processar base '{base_name}': {str(e)}"
        )

@router.get("/simple/{base_name}")
async def simple_test(base_name: str, motorista: str = Query("TESTE")):
    """Teste simples de busca"""
    try:
        return {
            "success": True,
            "base": base_name,
            "motorista": motorista,
            "message": "Teste funcionando"
        }
    except Exception as e:
        return {"error": str(e)}

@router.get("/test/{base_name}")
async def test_motoristas(base_name: str):
    """Testa busca de motoristas"""
    try:
        from app.services.database import db
        collection = db.database.sla_bases_data
        
        base_data = await collection.find_one({"base_name": base_name})
        if not base_data:
            return {"error": "Base não encontrada"}
        
        records = base_data.get("data", [])
        motoristas = set()
        for record in records:
            motorista = record.get("Responsável pela entrega", "")
            if motorista:
                motoristas.add(motorista)
        
        return {
            "success": True,
            "total_records": len(records),
            "total_motoristas": len(motoristas),
            "motoristas": list(sorted(motoristas))[:5]
        }
        
    except Exception as e:
        return {"error": str(e)}

