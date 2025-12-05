"""
Rotas para gerenciamento de snapshots
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import logging
from ..services.snapshot_service import SnapshotService

router = APIRouter(tags=["Reports - Snapshots"])
logger = logging.getLogger(__name__)


class CreateSnapshotRequest(BaseModel):
    """Request para criar snapshot"""
    module: str = "pedidos_parados"
    period_type: str = "manual"
    force: bool = False  # Forçar criação mesmo se já existir recente
    # Parâmetros específicos para SLA
    base: Optional[str] = None  # Base para SLA
    cities: Optional[List[str]] = None  # Cidades para SLA (se vazio, salva geral)
    custom_date: Optional[str] = None  # Data customizada para o snapshot (formato YYYY-MM-DD)


@router.post("/snapshot")
async def create_snapshot(request: CreateSnapshotRequest):
    """
    Cria um novo snapshot de dados
    
    - **module**: Módulo para criar snapshot (pedidos_parados, sla, d1)
    - **period_type**: Tipo de período (manual, daily, weekly, monthly)
    - **force**: Se True, força criação mesmo se já existir um snapshot recente
    """
    try:
        # Mapear módulo para coleção e método
        module_config = {
            "pedidos_parados": {
                "collection": "reports_snapshots",
                "method": SnapshotService.create_pedidos_parados_snapshot
            },
            "d1": {
                "collection": "d1_reports_snapshots",
                "method": SnapshotService.create_d1_snapshot
            },
            "sla": {
                "collection": "sla_reports_snapshots",
                "method": SnapshotService.create_sla_snapshot
            }
        }
        
        if request.module not in module_config:
            raise HTTPException(
                status_code=400,
                detail=f"Módulo '{request.module}' não suportado. Use: {', '.join(module_config.keys())}"
            )
        
        config = module_config[request.module]
        
        # Verificar se já existe snapshot recente (últimos 5 minutos)
        # Para SLA, verificar também base e cities para permitir múltiplos snapshots diferentes
        if not request.force:
            from datetime import datetime, timedelta
            from app.services.database import get_database
            
            db = get_database()
            if db is None:
                raise HTTPException(status_code=500, detail="Database não conectado")
            
            collection = db[config["collection"]]
            cinco_minutos_atras = datetime.now() - timedelta(minutes=5)
            
            # Construir query de verificação
            query = {
                "module": request.module,
                "snapshot_date": {"$gte": cinco_minutos_atras}
            }
            
            # Para SLA, adicionar filtros de base e cities para verificar duplicatas exatas
            # Isso permite múltiplos snapshots se base ou cities forem diferentes
            if request.module == "sla":
                and_conditions = [query]  # Começar com a query base
                
                # Adicionar filtro de base
                if request.base:
                    and_conditions.append({"base": request.base})
                else:
                    # Se não há base, verificar snapshots sem base (geral) ou null
                    and_conditions.append({
                        "$or": [
                            {"base": {"$exists": False}},
                            {"base": None}
                        ]
                    })
                
                # Adicionar filtro de cities
                if request.cities and len(request.cities) > 0:
                    # Ordenar cities para comparação consistente
                    cities_sorted = sorted(request.cities)
                    and_conditions.append({"cities": cities_sorted})
                else:
                    # Se não há cities, verificar snapshots sem cities (geral) ou com array vazio
                    and_conditions.append({
                        "$or": [
                            {"cities": {"$exists": False}},
                            {"cities": []},
                            {"cities": None}
                        ]
                    })
                
                # Se temos mais de uma condição, usar $and
                if len(and_conditions) > 1:
                    query = {"$and": and_conditions}
                else:
                    query = and_conditions[0]
            
            snapshot_recente = await collection.find_one(
                query,
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
        
        # Criar snapshot usando o método correto
        # Para SLA, passar base, cities e custom_date se fornecidos
        if request.module == "sla":
            result = await config["method"](base=request.base, cities=request.cities, custom_date=request.custom_date)
        else:
            result = await config["method"]()
        
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
        
        # Mapear módulo para coleção
        module_collections = {
            "pedidos_parados": "reports_snapshots",
            "d1": "d1_reports_snapshots",
            "sla": "sla_reports_snapshots"
        }
        
        if module not in module_collections:
            raise HTTPException(
                status_code=400,
                detail=f"Módulo '{module}' não suportado. Use: {', '.join(module_collections.keys())}"
            )
        
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não conectado")
        
        collection = db[module_collections[module]]
        
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


@router.get("/snapshots/all")
async def get_all_snapshots(module: str = "pedidos_parados"):
    """
    Retorna todos os snapshots de um módulo, ordenados por data (mais recente primeiro)
    """
    try:
        from app.services.database import get_database
        
        # Mapear módulo para coleção
        module_collections = {
            "pedidos_parados": "reports_snapshots",
            "d1": "d1_reports_snapshots",
            "sla": "sla_reports_snapshots"
        }
        
        if module not in module_collections:
            raise HTTPException(
                status_code=400,
                detail=f"Módulo '{module}' não suportado. Use: {', '.join(module_collections.keys())}"
            )
        
        db = get_database()
        if db is None:
            raise HTTPException(status_code=500, detail="Database não conectado")
        
        collection = db[module_collections[module]]
        
        # Buscar todos os snapshots, ordenados por data (mais recente primeiro)
        snapshots_cursor = collection.find({"module": module}).sort("snapshot_date", -1)
        snapshots = await snapshots_cursor.to_list(length=None)
        
        # Converter _id para string em cada snapshot
        for snapshot in snapshots:
            snapshot["_id"] = str(snapshot["_id"])
        
        return {
            "success": True,
            "data": snapshots,
            "total": len(snapshots)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar snapshots: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

