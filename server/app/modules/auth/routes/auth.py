"""
Rotas de Autenticação
"""
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import timedelta
from app.modules.auth.models.user import UserCreate, UserLogin, TokenResponse, UserResponse
from app.modules.auth.services.auth_service import (
    create_user,
    authenticate_user,
    create_access_token,
    verify_token,
    get_user_by_id,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter()
security = HTTPBearer()


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> UserResponse:
    """Dependency para obter usuário atual do token"""
    token = credentials.credentials
    payload = verify_token(token)
    
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido ou expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user_id = payload.get("sub")
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user = await get_user_by_id(user_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuário não encontrado",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return user


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(user_data: UserCreate):
    """Registra um novo usuário"""
    try:
        user = await create_user(user_data)
        logger.info(f"Usuário registrado: {user.nome}")
        return user
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Erro ao registrar usuário: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao registrar usuário"
        )


@router.post("/login", response_model=TokenResponse)
async def login(login_data: UserLogin):
    """Autentica um usuário e retorna token"""
    try:
        user = await authenticate_user(login_data.nome, login_data.senha)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Nome ou senha incorretos",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Garantir que id seja string
        # O id já deve estar como string do authenticate_user, mas vamos garantir
        if not user.id:
            logger.error(f"Usuário {login_data.nome} autenticado mas sem id válido. Tipo: {type(user.id)}, Valor: {user.id}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Erro interno: ID do usuário inválido"
            )
        
        user_id = str(user.id)
        
        # Definir tempo de expiração baseado em "lembrar"
        if login_data.lembrar:
            expires_delta = timedelta(days=30)  # 30 dias se lembrar
        else:
            expires_delta = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        
        # Criar token
        access_token = create_access_token(
            data={"sub": user_id, "nome": user.nome, "base": user.base},
            expires_delta=expires_delta
        )
        
        # Criar resposta do usuário
        user_response = UserResponse(
            id=user_id,
            nome=user.nome,
            base=user.base,
            created_at=user.created_at
        )
        
        logger.info(f"Usuário autenticado: {user.nome}")
        
        return TokenResponse(
            access_token=access_token,
            token_type="bearer",
            user=user_response
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao fazer login: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro interno ao fazer login: {str(e)}"
        )


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(current_user: UserResponse = Depends(get_current_user)):
    """Retorna informações do usuário atual"""
    return current_user


@router.post("/verify")
async def verify_token_endpoint(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verifica se o token é válido"""
    token = credentials.credentials
    payload = verify_token(token)
    
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido ou expirado"
        )
    
    return {"valid": True, "payload": payload}

