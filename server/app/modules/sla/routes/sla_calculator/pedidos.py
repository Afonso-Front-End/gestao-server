"""
Rotas de busca de pedidos para cálculo SLA
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from app.modules.sla.services.sla_calculator import SLACalculator

router = APIRouter(tags=["SLA Calculator - Pedidos"])

# Instância do calculador
sla_calculator = SLACalculator()

@router.get("/pedidos/{base_name}")
async def get_motorista_pedidos(
    base_name: str,
    motorista: str = Query(..., description="Nome do motorista"),
    status: Optional[str] = Query(None, description="Status: 'entregues' ou 'nao_entregues'"),
    cidade: Optional[str] = Query(None, description="Nome da cidade para filtrar (singular)"),
    cidades: Optional[List[str]] = Query(None, description="Lista de cidades para filtrar (múltiplas)")
):
    """
    Busca pedidos de um motorista específico
    
    Args:
        base_name: Nome da base
        motorista: Nome do motorista
        status: Status opcional (entregues/nao_entregues)
        cidade: Cidade opcional para filtrar (singular, para compatibilidade)
        cidades: Lista de cidades opcional para filtrar (múltiplas)
        
    Returns:
        Lista de pedidos do motorista
    """
    try:
        # Priorizar lista de cidades, senão usar cidade única
        cidades_filtro = cidades if cidades else ([cidade] if cidade else None)
        
        pedidos = await sla_calculator.get_motorista_pedidos(base_name, motorista, status, cidades_filtro)
        
        # Limpar dados para JSON (remover NaN)
        def clean_data(data):
            if isinstance(data, dict):
                return {k: clean_data(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [clean_data(item) for item in data]
            elif isinstance(data, float) and (data != data):  # NaN check
                return None
            else:
                return data
        
        pedidos_clean = clean_data(pedidos)
        
        return {
            "success": True,
            "data": pedidos_clean,
            "message": f"Encontrados {len(pedidos)} pedidos para o motorista '{motorista}'"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

