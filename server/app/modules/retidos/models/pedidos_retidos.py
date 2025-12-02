from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from bson import ObjectId

class PedidoRetidoItem(BaseModel):
    """Modelo flexível para qualquer item de dados do Excel"""
    # Aceita qualquer campo dinâmico do Excel
    def __init__(self, **data):
        super().__init__(**data)
    
    class Config:
        extra = "allow"  # Permite campos extras
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "qualquer_coluna": "qualquer_valor",
                "outra_coluna": "outro_valor",
                "numero_pedido": "123456",
                "base_origem": "SP-01"
            }
        }

class PedidosRetidosData(BaseModel):
    """Modelo principal para dados de pedidos retidos"""
    id: Optional[str] = Field(None, description="ID único do documento no MongoDB")
    filename: str = Field(..., description="Nome do arquivo Excel original", min_length=1)
    upload_date: datetime = Field(default_factory=datetime.now, description="Data e hora do upload")
    total_items: int = Field(..., description="Total de itens processados", ge=0)
    data: List[Dict[str, Any]] = Field(..., description="Lista de dados do Excel (qualquer estrutura)", min_items=1)
    columns_info: Optional[Dict[str, str]] = Field(None, description="Informações sobre as colunas do Excel")
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "filename": "pedidos_retidos_2025.xlsx",
                "upload_date": "2025-10-13T23:00:00",
                "total_items": 2,
                "data": [
                    {
                        "pedido": "123456789",
                        "base": "SP-01",
                        "entregador": "João Silva",
                        "qualquer_campo": "qualquer_valor"
                    },
                    {
                        "numero": "987654321",
                        "origem": "RJ-02",
                        "motorista": "Maria Santos",
                        "outro_campo": "outro_valor"
                    }
                ],
                "columns_info": {
                    "pedido": "Número do pedido",
                    "base": "Base de origem"
                }
            }
        }

class UploadResponse(BaseModel):
    """Resposta do upload de arquivo Excel"""
    success: bool = Field(..., description="Indica se o upload foi bem-sucedido")
    id: str = Field(..., description="ID único do documento salvo no MongoDB")
    total_items: int = Field(..., description="Total de itens processados", ge=0)
    filename: str = Field(..., description="Nome do arquivo original")
    message: str = Field(..., description="Mensagem de status do upload")
    columns_found: Optional[List[str]] = Field(None, description="Lista das colunas encontradas no Excel")
    data: Optional[List[Dict[str, Any]]] = Field(None, description="Dados processados do Excel")
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "id": "507f1f77bcf86cd799439011",
                "total_items": 150,
                "filename": "pedidos_retidos_2025.xlsx",
                "message": "Arquivo importado com sucesso! 150 registros processados.",
                "columns_found": ["pedido", "base", "entregador", "status"]
            }
        }

class ErrorResponse(BaseModel):
    """Modelo para respostas de erro"""
    success: bool = Field(False, description="Sempre false para erros")
    error: str = Field(..., description="Descrição do erro")
    detail: Optional[str] = Field(None, description="Detalhes adicionais do erro")
    
    class Config:
        schema_extra = {
            "example": {
                "success": False,
                "error": "Arquivo inválido",
                "detail": "O arquivo deve ser Excel (.xlsx, .xls)"
            }
        }

class TelefoneUpdateRequest(BaseModel):
    """Modelo para atualização de telefone do motorista"""
    base: str = Field(..., description="Base do motorista", min_length=1)
    motorista: str = Field(..., description="Nome do motorista", min_length=1)
    telefone: str = Field(..., description="Número de telefone", min_length=10, max_length=15)
    
    class Config:
        schema_extra = {
            "example": {
                "base": "CCM -SC",
                "motorista": "TAC JAIR NORONHA NUNES",
                "telefone": "11999999999"
            }
        }

# Alias para compatibilidade
RetidosData = PedidosRetidosData
RetidoItem = PedidoRetidoItem