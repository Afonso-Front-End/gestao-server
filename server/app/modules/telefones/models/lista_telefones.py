from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

class TelefoneMotorista(BaseModel):
    """Modelo totalmente dinâmico - aceita QUALQUER estrutura de dados"""
    
    class Config:
        extra = "allow"  # Permite QUALQUER campo
        json_schema_extra = {
            "example": {
                "qualquer_campo": "qualquer_valor",
                "outro_campo": 123,
                "mais_um": True
            }
        }

class ListaTelefonesRequest(BaseModel):
    """Modelo para receber lista de telefones"""
    telefones: List[TelefoneMotorista] = Field(..., description="Lista de telefones dos motoristas")
    origem: str = Field(default="frontend", description="Origem dos dados")
    timestamp: Optional[datetime] = Field(default_factory=datetime.now, description="Timestamp do envio")
    
    class Config:
        json_schema_extra = {
            "example": {
                "telefones": [
                    {
                        "motorista": "João Silva",
                        "base": "CCM -SC", 
                        "telefone": "47999999999",
                        "quantidade_pedidos": 5
                    },
                    {
                        "motorista": "Maria Santos",
                        "base": "CCM -SC",
                        "telefone": "47888888888", 
                        "quantidade_pedidos": 3
                    }
                ],
                "origem": "frontend",
                "timestamp": "2024-01-15T10:30:00"
            }
        }

class ListaTelefonesResponse(BaseModel):
    """Modelo para resposta da lista de telefones"""
    sucesso: bool = Field(..., description="Indica se a operação foi bem-sucedida")
    mensagem: str = Field(..., description="Mensagem de retorno")
    total_recebido: int = Field(..., description="Total de telefones recebidos")
    timestamp: datetime = Field(default_factory=datetime.now, description="Timestamp da resposta")
    
    class Config:
        json_schema_extra = {
            "example": {
                "sucesso": True,
                "mensagem": "Lista de telefones recebida com sucesso",
                "total_recebido": 2,
                "timestamp": "2024-01-15T10:30:00"
            }
        }
