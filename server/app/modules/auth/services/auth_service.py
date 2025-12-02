"""
Serviço de Autenticação
"""
import bcrypt
from datetime import datetime, timedelta
from jose import JWTError, jwt
from typing import Optional
import os
import logging
from app.services.database import get_database
from app.core.collections import COLLECTION_USERS
from app.modules.auth.models.user import UserInDB, UserCreate, UserResponse

logger = logging.getLogger(__name__)

# Configurações JWT
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "sua-chave-secreta-super-segura-aqui-mude-em-producao")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "1440"))  # 24 horas padrão


def hash_password(password: str) -> str:
    """Gera hash da senha usando bcrypt"""
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifica se a senha está correta"""
    try:
        return bcrypt.checkpw(
            plain_password.encode('utf-8'),
            hashed_password.encode('utf-8')
        )
    except Exception as e:
        logger.error(f"Erro ao verificar senha: {e}")
        return False


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Cria token JWT"""
    try:
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt
    except Exception as e:
        logger.error(f"Erro ao criar token JWT: {e}")
        raise


def verify_token(token: str) -> Optional[dict]:
    """Verifica e decodifica token JWT"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError as e:
        logger.warning(f"Token inválido: {e}")
        return None


async def create_user(user_data: UserCreate) -> UserResponse:
    """Cria um novo usuário"""
    try:
        db = get_database()
        collection = db[COLLECTION_USERS]
        
        # Verificar se usuário já existe
        existing_user = await collection.find_one({"nome": user_data.nome})
        if existing_user:
            raise ValueError("Usuário já existe")
        
        # Criar hash da senha
        senha_hash = hash_password(user_data.senha)
        
        # Criar documento do usuário
        user_doc = {
            "nome": user_data.nome,
            "base": user_data.base,
            "senha_hash": senha_hash,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        # Inserir no banco
        result = await collection.insert_one(user_doc)
        user_id = str(result.inserted_id)
        
        logger.info(f"Usuário criado: {user_data.nome}")
        
        return UserResponse(
            id=user_id,
            nome=user_data.nome,
            base=user_data.base,
            created_at=user_doc["created_at"]
        )
    except Exception as e:
        logger.error(f"Erro ao criar usuário: {e}")
        raise


async def authenticate_user(nome: str, senha: str) -> Optional[UserInDB]:
    """Autentica um usuário"""
    try:
        db = get_database()
        collection = db[COLLECTION_USERS]
        
        # Buscar usuário
        user_doc = await collection.find_one({"nome": nome})
        if not user_doc:
            return None
        
        # Verificar senha
        if not verify_password(senha, user_doc.get("senha_hash", "")):
            return None
        
        # Garantir que _id existe e converter ObjectId para string
        from bson import ObjectId
        
        # Acessar _id - MongoDB sempre retorna _id como ObjectId
        user_id = user_doc.get("_id")
        
        # Log para debug
        logger.debug(f"Usuário {nome}: _id obtido do documento, tipo: {type(user_id)}, valor: {user_id}")
        
        # Se não encontrou, tentar acesso direto
        if user_id is None:
            try:
                user_id = user_doc["_id"]
                logger.debug(f"Usuário {nome}: _id obtido via acesso direto, tipo: {type(user_id)}, valor: {user_id}")
            except (KeyError, TypeError) as e:
                logger.error(f"Usuário {nome} encontrado mas sem _id. Erro: {e}")
                return None
        
        # Converter ObjectId para string (sempre necessário)
        # O MongoDB sempre retorna _id como ObjectId, então sempre converter
        try:
            if isinstance(user_id, ObjectId):
                user_id = str(user_id)
                logger.debug(f"Usuário {nome}: _id convertido de ObjectId para string: {user_id}")
            elif user_id is not None:
                user_id = str(user_id)
                logger.debug(f"Usuário {nome}: _id convertido para string: {user_id}")
            else:
                logger.error(f"Usuário {nome}: _id é None")
                return None
        except Exception as e:
            logger.error(f"Erro ao converter _id para string: {e}, tipo: {type(user_id)}, valor: {user_id}")
            return None
        
        # Converter para modelo
        # Log para verificar valores antes de criar UserInDB
        logger.info(f"Usuário {nome}: Criando UserInDB com _id={user_id}, tipo={type(user_id)}")
        
        # Criar dicionário com os dados
        # Usar _id para o alias do Pydantic
        user_data = {
            "_id": user_id,
            "nome": user_doc.get("nome", ""),
            "base": user_doc.get("base", ""),
            "senha_hash": user_doc.get("senha_hash", ""),
            "created_at": user_doc.get("created_at"),
            "updated_at": user_doc.get("updated_at")
        }
        
        logger.info(f"Usuário {nome}: Dados para UserInDB: {user_data}")
        
        user = UserInDB(**user_data)
        
        # Verificar se o id foi salvo corretamente (agora é 'id' não '_id')
        logger.info(f"Usuário {nome}: UserInDB criado com id={user.id}, tipo={type(user.id)}")
        
        if not user.id:
            logger.error(f"Usuário {nome}: UserInDB criado mas id está None! Dados: {user_data}")
            return None
        
        return user
    except Exception as e:
        logger.error(f"Erro ao autenticar usuário: {e}", exc_info=True)
        return None


async def get_user_by_id(user_id: str) -> Optional[UserResponse]:
    """Busca usuário por ID"""
    try:
        from bson import ObjectId
        db = get_database()
        collection = db[COLLECTION_USERS]
        
        user_doc = await collection.find_one({"_id": ObjectId(user_id)})
        if not user_doc:
            return None
        
        return UserResponse(
            id=str(user_doc["_id"]),
            nome=user_doc["nome"],
            base=user_doc["base"],
            created_at=user_doc.get("created_at")
        )
    except Exception as e:
        logger.error(f"Erro ao buscar usuário: {e}")
        return None

