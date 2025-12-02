from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
from bson import ObjectId

class SLAChunk(BaseModel):
    """Modelo para chunks de dados SLA"""
    id: Optional[str] = Field(None, alias="_id")
    chunk_index: int = Field(..., description="Índice do chunk")
    total_chunks: int = Field(..., description="Total de chunks")
    file_id: str = Field(..., description="ID do arquivo original")
    data: List[Dict[str, Any]] = Field(..., description="Dados do chunk")
    created_at: datetime = Field(default_factory=datetime.now)
    status: str = Field(default="completed", description="Status do chunk")
    
    class Config:
        allow_population_by_field_name = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat()
        }

class SLAFile(BaseModel):
    """Modelo para arquivo SLA processado"""
    id: Optional[str] = Field(None, alias="_id")
    filename: str = Field(..., description="Nome do arquivo")
    file_size: int = Field(..., description="Tamanho do arquivo em bytes")
    total_chunks: int = Field(..., description="Total de chunks criados")
    total_records: int = Field(..., description="Total de registros processados")
    unique_bases: List[str] = Field(default=[], description="Lista de bases únicas encontradas")
    created_at: datetime = Field(default_factory=datetime.now)
    status: str = Field(default="completed", description="Status do arquivo")
    
    class Config:
        allow_population_by_field_name = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat()
        }

class SLAStats(BaseModel):
    """Modelo para estatísticas de SLA"""
    total_files: int = Field(default=0)
    total_chunks: int = Field(default=0)
    total_records: int = Field(default=0)
    last_processed: Optional[datetime] = Field(None)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
