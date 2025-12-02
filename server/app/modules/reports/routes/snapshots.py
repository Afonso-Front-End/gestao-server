"""
Rotas para gerenciamento de snapshots
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import logging
from ..services.snapshot_service import SnapshotService

router = APIRouter(tags=["Reports - Snapshots"])
logger = logging.getLogger(__name__)


class CreateSnapshotRequest(BaseModel):
    """Request para criar snapshot"""
    module: str = "pedidos_parados"
    period_type: str = "manual"
    force: bool = False  # Forçar criação mesmo se já existir recente


@router.post("/snapshot")
async def create_snapshot(request: CreateSnapshotRequest):
    """
    Cria um novo snapshot de dados
    
    - **module**: Módulo para criar snapshot (pedidos_parados, sla, d1)
    - **period_type**: Tipo de período (manual, daily, weekly, monthly)
    - **force**: Se True, força criação mesmo se já existir um snapshot recente
    """
    try:
        if request.module != "pedidos_parados":
            raise HTTPException(
                status_code=400,
                detail=f"Módulo '{request.module}' ainda não suportado. Use 'pedidos_parados'."
            )
        
        # Verificar se já existe snapshot recente (últimos 5 minutos)
        if not request.force:
            from datetime import datetime, timedelta
            from app.services.database import get_database
            
            db = get_database()
            if db is None:
                raise HTTPException(status_code=500, detail="Database não conectado")
            
            collection = db["reports_snapshots"]
            cinco_minutos_atras = datetime.now() - timedelta(minutes=5)
            
            snapshot_recente = await collection.find_one(
                {
                    "module": request.module,
                    "snapshot_date": {"$gte": cinco_minutos_atras}
                },
                sort=[("snapshot_date", -1)]
            )
            
            if snapshot_recente:
                logger.info(f"⚠️ Snapshot recente já existe (criado em {snapshot_recente.get('snapshot_date')})")
                
                # Retornar o snapshot existente
                return {
                    "success": True,
                    "message": "Snapshot recente já existe! Usando snapshot existente.",
                    "is_duplicate": True,
                    "data": {
                        "success": True,
                        "snapshot_id": str(snapshot_recente.get("_id")),
                        "metrics": snapshot_recente.get("metrics", {}),
                        "snapshot_date": snapshot_recente.get("snapshot_date")
                    }
                }
        
        # Criar snapshot
        result = await SnapshotService.create_pedidos_parados_snapshot()
        
        logger.info(f"✅ Snapshot criado! Total pedidos: {result.get('metrics', {}).get('total_pedidos', 0)}")
        
        return {
            "success": True,
            "message": "Snapshot criado com sucesso!",
            "is_duplicate": False,
            "data": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao criar snapshot: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/snapshots/latest")
async def get_latest_snapshot(module: str = "pedidos_parados"):
    """
    Retorna o snapshot mais recente de um módulo
    """
    try:
        from app.services.database import get_database
        
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não conectado")
        
        collection = db["reports_snapshots"]
        
        # Buscar snapshot mais recente
        snapshot = await collection.find_one(
            {"module": module},
            sort=[("snapshot_date", -1)]
        )
        
        if not snapshot:
            return {
                "success": True,
                "message": "Nenhum snapshot encontrado",
                "data": None
            }
        
        # Converter _id para string
        snapshot["_id"] = str(snapshot["_id"])
        
        return {
            "success": True,
            "data": snapshot
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar snapshot: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

