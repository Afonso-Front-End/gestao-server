"""
Modelo de Usuário
"""
from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime
from bson import ObjectId


class UserBase(BaseModel):
    """Modelo base de usuário"""
    nome: str
    base: str
    senha: str


class UserCreate(UserBase):
    """Modelo para criação de usuário"""
    pass


class UserInDB(BaseModel):
    """Modelo de usuário no banco de dados"""
    id: str = Field(alias="_id")  # Usar alias para _id do MongoDB
    nome: str
    base: str
    senha_hash: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {
        "populate_by_name": True,  # Permite usar tanto _id quanto id
        "arbitrary_types_allowed": True
    }


class UserResponse(BaseModel):
    """Modelo de resposta de usuário (sem senha)"""
    id: str
    nome: str
    base: str
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class UserLogin(BaseModel):
    """Modelo para login"""
    nome: str
    senha: str
    lembrar: bool = False


class TokenResponse(BaseModel):
    """Modelo de resposta de token"""
    access_token: str
    token_type: str = "bearer"
    user: UserResponse

