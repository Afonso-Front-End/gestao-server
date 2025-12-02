from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from app.core.collections import COLLECTION_TELEFONES

logger = logging.getLogger(__name__)

class ListaTelefonesService:
    """Serviço para gerenciar operações de lista de telefones no MongoDB"""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.collection = db[COLLECTION_TELEFONES]
    
    async def salvar_lista_telefones(
        self, 
        timestamp: str, 
        origem: str, 
        dados_processados: List[Dict[str, Any]],
        estatisticas: Dict[str, Any]
    ) -> str:
        """
        Salva uma lista de telefones processados no MongoDB
        Args:
            timestamp: Timestamp do envio
            origem: Origem dos dados
            dados_processados: Lista de dados processados do Excel
            estatisticas: Estatísticas do processamento
        Returns:
            str: ID do documento salvo
        """
        try:
            documento = {
                "timestamp": timestamp,
                "origem": origem,
                "total_linhas": len(dados_processados),
                "dados_processados": dados_processados,
                "estatisticas": estatisticas
            }
            resultado = await self.collection.insert_one(documento)
            documento_id = str(resultado.inserted_id)
            return documento_id
        except Exception as e:
            logger.error(f"❌ Erro ao salvar lista de telefones: {str(e)}")
            raise e
    
    async def buscar_por_id(self, documento_id: str) -> Optional[Dict[str, Any]]:
        """
        Busca uma lista de telefones por ID
        Args:
            documento_id: ID do documento
        Returns:
            Dict com os dados do documento ou None se não encontrado
        """
        try:
            from bson import ObjectId
            documento = await self.collection.find_one({"_id": ObjectId(documento_id)})
            if documento:
                documento["_id"] = str(documento["_id"])
            return documento
        except Exception as e:
            logger.error(f"❌ Erro ao buscar documento: {str(e)}")
            raise e
    
    async def listar_todas_listas(
        self, 
        limite: int = 50, 
        pular: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Lista todas as listas de telefones salvas
        Args:
            limite: Número máximo de documentos a retornar
            pular: Número de documentos a pular (para paginação)
        Returns:
            Lista de documentos
        """
        try:
            cursor = self.collection.find().sort("processado_em", -1).skip(pular).limit(limite)
            documentos = await cursor.to_list(length=limite)
            # Converter ObjectId para string
            for doc in documentos:
                doc["_id"] = str(doc["_id"])
            return documentos
        except Exception as e:
            logger.error(f"❌ Erro ao listar documentos: {str(e)}")
            raise e
    
    async def contar_total_documentos(self) -> int:
        """
        Conta o total de documentos na coleção
        Returns:
            Número total de documentos
        """
        try:
            total = await self.collection.count_documents({})
            return total
        except Exception as e:
            logger.error(f"❌ Erro ao contar documentos: {str(e)}")
            raise e
    
    async def deletar_por_id(self, documento_id: str) -> bool:
        """
        Deleta um documento por ID
        Args:
            documento_id: ID do documento
        Returns:
            True se deletado com sucesso, False caso contrário
        """
        try:
            from bson import ObjectId
            resultado = await self.collection.delete_one({"_id": ObjectId(documento_id)})
            if resultado.deleted_count > 0:
                return True
            else:
                return False
        except Exception as e:
            logger.error(f"❌ Erro ao deletar documento: {str(e)}")
            raise e