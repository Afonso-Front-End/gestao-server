"""
Rotas para dashboard de reports
"""
from fastapi import APIRouter

router = APIRouter(tags=["Reports - Dashboard"])


@router.get("/dashboard")
async def get_dashboard_data(module: str = "pedidos_parados", period: str = "last_7_days"):
    """
    Retorna dados para o dashboard
    
    - **module**: Módulo (pedidos_parados, sla, d1)
    - **period**: Período (today, last_7_days, last_30_days, custom)
    """
    # TODO: Implementar busca de dados do dashboard
    return {
        "success": True,
        "message": "Dashboard em desenvolvimento",
        "data": {
            "module": module,
            "period": period
        }
    }

