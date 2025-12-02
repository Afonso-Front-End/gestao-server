"""
Gerador de API Keys para o servidor
"""
import hmac
import hashlib
import secrets
import os
import sys

# Chave padrão de desenvolvimento (mesma do security.py)
DEFAULT_DEV_SECRET_KEY = "dev_secret_key_do_not_use_in_production_change_this_immediately"

def generate_api_key(secret_key: str) -> dict:
    """
    Gera API Key e Secret a partir de uma chave secreta
    
    Args:
        secret_key: Chave secreta (deve ser mantida em segredo)
    
    Returns:
        dict com 'api_key' e 'api_secret'
    """
    # Gerar API Key (HMAC-SHA256 da chave secreta com salt 'api_key')
    api_key = hmac.new(
        secret_key.encode('utf-8'),
        b'api_key',
        hashlib.sha256
    ).hexdigest()
    
    # Gerar API Secret (HMAC-SHA256 da chave secreta com salt 'api_secret')
    api_secret = hmac.new(
        secret_key.encode('utf-8'),
        b'api_secret',
        hashlib.sha256
    ).hexdigest()
    
    return {
        "api_key": api_key,
        "api_secret": api_secret
    }


def generate_secret_key() -> str:
    """
    Gera uma chave secreta aleatória segura
    
    Returns:
        String com chave secreta (32 bytes em hex = 64 caracteres)
    """
    return secrets.token_hex(32)


if __name__ == "__main__":
    # Script para gerar chaves
    
    # Opção especial: gerar chaves para desenvolvimento padrão
    if len(sys.argv) > 1 and sys.argv[1] == "--dev":
        credentials = generate_api_key(DEFAULT_DEV_SECRET_KEY)
        print("\n" + "="*60)
        print("Chaves de DESENVOLVIMENTO (Padrao)")
        print("="*60)
        print("\nATENCAO: Estas sao chaves de desenvolvimento padrao!")
        print("   Use apenas para desenvolvimento local.")
        print("   Para producao, gere chaves unicas com: python -m app.core.api_key_generator")
        print("\n" + "="*60)
        print(f"\nAPI Key:   {credentials['api_key']}")
        print(f"API Secret: {credentials['api_secret']}")
        print("\n" + "="*60)
        print("\nConfigure no frontend (app/.env):")
        print(f"VITE_API_KEY={credentials['api_key']}")
        print(f"VITE_API_SECRET={credentials['api_secret']}")
        print("\n" + "="*60)
        print("\nO servidor ja esta configurado com a chave padrao.")
        print("   Nao precisa configurar API_SECRET_KEY no servidor para desenvolvimento.")
        print("\n" + "="*60)
        sys.exit(0)
    
    if len(sys.argv) > 1:
        # Se forneceu uma chave secreta, gerar API Key/Secret
        secret_key = sys.argv[1]
        credentials = generate_api_key(secret_key)
        print("\n" + "="*60)
        print("API Credentials Geradas")
        print("="*60)
        print(f"\nAPI Key:   {credentials['api_key']}")
        print(f"API Secret: {credentials['api_secret']}")
        print("\n" + "="*60)
        print("\nConfigure no servidor (.env):")
        print(f"API_SECRET_KEY={secret_key}")
        print("\nConfigure no frontend (.env ou localStorage):")
        print(f"VITE_API_KEY={credentials['api_key']}")
        print(f"VITE_API_SECRET={credentials['api_secret']}")
        print("\n" + "="*60)
    else:
        # Gerar chave secreta nova
        secret_key = generate_secret_key()
        credentials = generate_api_key(secret_key)
        print("\n" + "="*60)
        print("Nova Chave Secreta e Credenciais Geradas")
        print("="*60)
        print(f"\n🔑 Secret Key: {secret_key}")
        print(f"\n🔐 API Key:   {credentials['api_key']}")
        print(f"🔐 API Secret: {credentials['api_secret']}")
        print("\n" + "="*60)
        print("\n📝 Configure no servidor (server/.env):")
        print(f"API_SECRET_KEY={secret_key}")
        print("\n📝 Configure no frontend (app/.env):")
        print(f"VITE_API_KEY={credentials['api_key']}")
        print(f"VITE_API_SECRET={credentials['api_secret']}")
        print("\n" + "="*60)
        print("\n⚠️ IMPORTANTE: A segurança está SEMPRE ATIVA!")
        print("   Se não configurar as chaves, use a chave padrão de desenvolvimento.")
        print("\n" + "="*60)

