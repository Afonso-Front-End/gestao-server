"""
Modelos Pydantic para D-1
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class UploadResponse(BaseModel):
    """Resposta do upload"""
    success: bool
    message: str
    id: Optional[str] = None
    filename: Optional[str] = None
    total_items: Optional[int] = None
    total_chunks: Optional[int] = None
    processing_time: Optional[float] = None


class D1MainDocument(BaseModel):
    """Documento principal D-1"""
    filename: str
    upload_date: datetime
    total_items: int
    chunk_size: int
    total_chunks: int
    columns_found: List[str]
    status: str = "processing"  # processing, completed, error
    error_message: Optional[str] = None
    processing_time: Optional[float] = None


class D1ChunkDocument(BaseModel):
    """Documento de chunk D-1"""
    main_document_id: str
    chunk_number: int
    chunk_data: List[Dict[str, Any]]
    chunk_size: int
    upload_date: datetime


