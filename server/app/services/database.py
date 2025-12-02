from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from app.core.collections import (
    COLLECTION_PEDIDOS_RETIDOS,
    COLLECTION_PEDIDOS_RETIDOS_CHUNKS,
    COLLECTION_PEDIDOS_RETIDOS_TABELA,
    COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS,
    COLLECTION_D1_MAIN,
    COLLECTION_D1_CHUNKS
)

# Configurações do banco de dados
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("DATABASE_NAME", "bdlogistica")

logger = logging.getLogger(__name__)

class Database:
    client: AsyncIOMotorClient = None
    database = None

db = Database()

async def connect_to_mongo():
    """Conecta ao MongoDB"""
    try:
        db.client = AsyncIOMotorClient(MONGODB_URL)
        db.database = db.client[DATABASE_NAME]
        # Testar conexão
        await db.client.admin.command('ping')
    except Exception as e:
        logger.error(f"Erro ao conectar ao MongoDB: {e}")
        raise

async def close_mongo_connection():
    """Fecha conexão com MongoDB"""
    if db.client:
        db.client.close()

async def insert_pedidos_retidos(data: dict) -> str:
    """Insere dados na coleção pedidos_retidos"""
    try:
        collection = db.database[COLLECTION_PEDIDOS_RETIDOS]
        result = await collection.insert_one(data)
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"Erro ao inserir dados: {e}")
        raise

async def get_pedidos_retidos(data_id: str) -> dict:
    """Busca dados na coleção pedidos_retidos por ID"""
    try:
        from bson import ObjectId
        collection = db.database.pedidos_retidos
        document = await collection.find_one({"_id": ObjectId(data_id)})
        return document
    except Exception as e:
        logger.error(f"Erro ao buscar dados: {e}")
        raise

async def get_all_pedidos_retidos() -> list:
    """Busca todos os dados da coleção pedidos_retidos"""
    try:
        collection = db.database.pedidos_retidos
        cursor = collection.find({})
        data_list = []
        async for document in cursor:
            document['id'] = str(document['_id'])
            del document['_id']
            data_list.append(document)
        return data_list
    except Exception as e:
        logger.error(f"Erro ao buscar todos os dados: {e}")
        raise

async def insert_pedidos_retidos_chunk(data: dict) -> str:
    """Insere dados de chunk na coleção pedidos_retidos_chunks"""
    try:
        collection = db.database[COLLECTION_PEDIDOS_RETIDOS_CHUNKS]
        result = await collection.insert_one(data)
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"Erro ao inserir chunk: {e}")
        raise

async def update_pedidos_retidos_status(document_id: str, status: str) -> bool:
    """Atualiza o status de um documento principal"""
    try:
        from bson import ObjectId
        collection = db.database.pedidos_retidos
        result = await collection.update_one(
            {"_id": ObjectId(document_id)},
            {"$set": {"status": status}}
        )
        return result.modified_count > 0
    except Exception as e:
        logger.error(f"Erro ao atualizar status: {e}")
        raise

async def get_pedidos_retidos_chunks(main_document_id: str) -> list:
    """Busca todos os chunks de um documento principal"""
    try:
        collection = db.database[COLLECTION_PEDIDOS_RETIDOS_CHUNKS]
        cursor = collection.find({"main_document_id": main_document_id}).sort("chunk_number", 1)
        chunks = []
        async for document in cursor:
            document['id'] = str(document['_id'])
            del document['_id']
            chunks.append(document)
        return chunks
    except Exception as e:
        logger.error(f"Erro ao buscar chunks: {e}")
        raise

def get_database():
    """Retorna a instância do database"""
    return db.database

# ===== FUNÇÕES PARA TABELA DE DADOS =====

async def insert_tabela_dados(document):
    """Insere documento principal da tabela de dados"""
    try:
        collection = db.database[COLLECTION_PEDIDOS_RETIDOS_TABELA]
        result = await collection.insert_one(document)
        return result.inserted_id
    except Exception as e:
        logger.error(f"Erro ao inserir tabela de dados: {e}")
        raise

async def insert_tabela_dados_chunk(chunk_document):
    """Insere chunk da tabela de dados"""
    try:
        collection = db.database[COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS]
        await collection.insert_one(chunk_document)
    except Exception as e:
        logger.error(f"Erro ao inserir chunk da tabela de dados: {e}")
        raise

async def update_tabela_dados_status(main_id, status):
    """Atualiza status da tabela de dados"""
    try:
        collection = db.database[COLLECTION_PEDIDOS_RETIDOS_TABELA]
        await collection.update_one(
            {"_id": main_id},
            {"$set": {"status": status}}
        )
    except Exception as e:
        logger.error(f"Erro ao atualizar status da tabela de dados: {e}")
        raise

async def get_tabela_dados_chunks(main_id):
    """Busca todos os chunks de uma tabela de dados"""
    try:
        collection = db.database[COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS]
        cursor = collection.find({"main_id": main_id}).sort("chunk_number", 1)
        chunks = []
        async for document in cursor:
            document['id'] = str(document['_id'])
            del document['_id']
            chunks.append(document)
        return chunks
    except Exception as e:
        logger.error(f"Erro ao buscar chunks da tabela de dados: {e}")
        raise

async def clear_tabela_dados_collections():
    """Limpa todas as coleções de tabela de dados (main + chunks)"""
    try:
        # Deletar todos os documentos principais
        main_collection = db.database[COLLECTION_PEDIDOS_RETIDOS_TABELA]
        main_result = await main_collection.delete_many({})
        
        # Deletar todos os chunks
        chunks_collection = db.database[COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS]
        chunks_result = await chunks_collection.delete_many({})
        
        logger.info(f"🗑️ Limpeza concluída: {main_result.deleted_count} docs principais e {chunks_result.deleted_count} chunks removidos")
        
        return {
            "main_deleted": main_result.deleted_count,
            "chunks_deleted": chunks_result.deleted_count
        }
    except Exception as e:
        logger.error(f"Erro ao limpar coleções de tabela de dados: {e}")
        raise

# ===== FUNÇÕES PARA D-1 =====

async def insert_d1_main(document: dict) -> str:
    """Insere documento principal D-1"""
    try:
        collection = db.database[COLLECTION_D1_MAIN]
        result = await collection.insert_one(document)
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"Erro ao inserir documento principal D-1: {e}")
        raise

async def insert_d1_chunk(chunk_document: dict) -> str:
    """Insere chunk D-1 de forma otimizada (bulk insert quando possível)"""
    try:
        collection = db.database[COLLECTION_D1_CHUNKS]
        chunk_number = chunk_document.get('chunk_number', '?')
        logger.debug(f"💾 Inserindo chunk {chunk_number} individualmente...")
        result = await collection.insert_one(chunk_document)
        logger.debug(f"✅ Chunk {chunk_number} inserido com ID: {result.inserted_id}")
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"❌ Erro ao inserir chunk D-1 (chunk {chunk_document.get('chunk_number', '?')}): {e}")
        logger.exception(e)
        raise

async def insert_d1_chunks_bulk(chunks: list) -> int:
    """Insere múltiplos chunks D-1 de uma vez (otimizado para grandes volumes)"""
    try:
        if not chunks:
            logger.warning("⚠️ Tentativa de inserir lista vazia de chunks")
            return 0
        
        collection = db.database[COLLECTION_D1_CHUNKS]
        logger.info(f"💾 Inserindo {len(chunks)} chunks em bulk na coleção {COLLECTION_D1_CHUNKS}...")
        result = await collection.insert_many(chunks, ordered=False)
        inserted_count = len(result.inserted_ids)
        logger.info(f"✅ {inserted_count} chunks inseridos com sucesso")
        return inserted_count
    except Exception as e:
        logger.error(f"❌ Erro ao inserir chunks D-1 em bulk: {e}")
        logger.exception(e)
        raise

async def update_d1_status(document_id: str, status: str, error_message: str = None, processing_time: float = None) -> bool:
    """Atualiza status do documento principal D-1"""
    try:
        from bson import ObjectId
        collection = db.database[COLLECTION_D1_MAIN]
        update_data = {"status": status}
        if error_message:
            update_data["error_message"] = error_message
        if processing_time is not None:
            update_data["processing_time"] = processing_time
        
        result = await collection.update_one(
            {"_id": ObjectId(document_id)},
            {"$set": update_data}
        )
        return result.modified_count > 0
    except Exception as e:
        logger.error(f"Erro ao atualizar status D-1: {e}")
        raise