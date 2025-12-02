"""
Modelo de Snapshot de Dados para Reports
"""
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime


class BaseMetrics(BaseModel):
    """Métricas por base"""
    base: str
    total: int
    entregues: int
    nao_entregues: int
    taxa_entrega: float = 0.0


class MotoristaMetrics(BaseModel):
    """Métricas por motorista"""
    motorista: str
    total: int
    entregues: int
    nao_entregues: int
    taxa_entrega: float = 0.0


class CidadeMetrics(BaseModel):
    """Métricas por cidade"""
    cidade: str
    total: int
    entregues: int
    nao_entregues: int
    taxa_entrega: float = 0.0


class AgingMetrics(BaseModel):
    """Métricas por aging"""
    aging: str
    total: int


class ContatoMetrics(BaseModel):
    """Métricas de status de contato"""
    retornou: int = 0
    nao_retornou: int = 0
    esperando_retorno: int = 0
    numero_errado: int = 0


class SnapshotMetrics(BaseModel):
    """Estrutura completa de métricas do snapshot"""
    # Métricas gerais
    total_pedidos: int
    total_motoristas: int
    total_bases: int
    total_cidades: int
    
    # Status de entrega
    entregues: int
    nao_entregues: int
    taxa_entrega: float
    
    # Status de contato
    contatos: ContatoMetrics
    
    # Distribuições
    por_base: List[BaseMetrics]
    top_cidades: List[CidadeMetrics]
    top_motoristas: List[MotoristaMetrics]
    por_aging: List[AgingMetrics]


class ReportSnapshot(BaseModel):
    """Modelo principal de Snapshot"""
    snapshot_date: datetime = Field(default_factory=datetime.now)
    module: str = "pedidos_parados"
    period_type: str = "manual"  # manual, daily, weekly, monthly
    metrics: SnapshotMetrics
    created_by: str = "system"
    
    class Config:
        json_schema_extra = {
            "example": {
                "module": "pedidos_parados",
                "period_type": "manual",
                "metrics": {
                    "total_pedidos": 1543,
                    "total_motoristas": 45,
                    "total_bases": 12,
                    "total_cidades": 28,
                    "entregues": 891,
                    "nao_entregues": 652,
                    "taxa_entrega": 57.7
                }
            }
        }

