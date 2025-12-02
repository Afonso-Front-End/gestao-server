from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from bson import ObjectId

class GalpaoEntrada(BaseModel):
    """Modelo para entradas no galpão"""
    id: Optional[str] = Field(None, alias="_id")
    pedido_jms: str = Field(..., description="Número do pedido JMS")
    motorista: str = Field(..., description="Nome do motorista")
    base_name: str = Field(..., description="Nome da base")
    data_entrada: datetime = Field(..., description="Data e hora da entrada no galpão")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Data de criação do registro")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Data de atualização")

    class Config:
        allow_population_by_field_name = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat()
        }

class GalpaoEntradaCreate(BaseModel):
    """Modelo para criação de entrada no galpão"""
    pedido_jms: str
    motorista: str
    base_name: str
    data_entrada: datetime

class GalpaoEntradaResponse(BaseModel):
    """Modelo para resposta da API"""
    success: bool
    data: Optional[list] = None
    message: str
    total_entradas: Optional[int] = None
