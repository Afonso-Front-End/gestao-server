"""
Modelo para dados de bipagens em tempo real
"""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from bson import ObjectId

class BipagemData(BaseModel):
    """Modelo para dados de bipagens processados"""
    id: Optional[str] = Field(None, alias="_id")
    numero_pedido_jms: str = Field(..., description="Número de pedido JMS")
    base_entrega: str = Field(..., description="Base de entrega")
    horario_saida_entrega: Optional[str] = Field(None, description="Horário de saída para entrega")
    responsavel_entrega: str = Field(..., description="Responsável pela entrega")
    marca_assinatura: str = Field(..., description="Marca de assinatura")
    cep_destino: Optional[str] = Field(None, description="CEP destino")
    motivos_pacotes_problematicos: Optional[str] = Field(None, description="Motivos dos pacotes problemáticos")
    destinatario: Optional[str] = Field(None, description="Destinatário")
    complemento: Optional[str] = Field(None, description="Complemento")
    distrito_destinatario: Optional[str] = Field(None, description="Distrito destinatário")
    cidade_destino: Optional[str] = Field(None, description="Cidade Destino")
    tres_segmentos: Optional[str] = Field(None, description="3 Segmentos")
    tempo_digitalizacao: Optional[datetime] = Field(None, description="Tempo de digitalização (data mais recente)")
    tempo_pedido_parado: Optional[str] = Field(None, description="Tempo de pedido parado (Exceed X days with no track)")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    
    class Config:
        allow_population_by_field_name = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat() if v else None
        }

