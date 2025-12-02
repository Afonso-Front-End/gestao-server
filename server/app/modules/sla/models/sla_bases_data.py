from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
from bson import ObjectId

class SLABaseData(BaseModel):
    """Modelo para dados de uma base específica"""
    id: Optional[str] = Field(None, alias="_id")
    base_name: str = Field(..., description="Nome da base")
    total_records: int = Field(..., description="Total de registros da base")
    total_pedidos: int = Field(..., description="Total de pedidos únicos")
    data: List[Dict[str, Any]] = Field(..., description="Dados da base")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    status: str = Field(default="processed", description="Status do processamento")
    
    class Config:
        allow_population_by_field_name = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat()
        }

class SLABaseStats(BaseModel):
    """Modelo para estatísticas de uma base"""
    base_name: str = Field(..., description="Nome da base")
    total_records: int = Field(default=0)
    total_pedidos: int = Field(default=0)
    last_processed: Optional[datetime] = Field(None)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
