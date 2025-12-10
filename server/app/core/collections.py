"""
Constantes para nomes das coleções do MongoDB
Padronização centralizada para facilitar manutenção

PADRÃO:
- {modulo} = coleção principal/resumo
- {modulo}_chunks = coleção de chunks
- {modulo}_{tipo} = coleções específicas
"""

# ========================================
# AUTENTICAÇÃO
# ========================================
COLLECTION_USERS = "users"

# ========================================
# LISTA DE TELEFONES
# ========================================
COLLECTION_TELEFONES = "telefones"

# ========================================
# PEDIDOS RETIDOS
# ========================================
COLLECTION_PEDIDOS_RETIDOS = "pedidos_retidos"  # Resumo/metadados principais
COLLECTION_PEDIDOS_RETIDOS_CHUNKS = "pedidos_retidos_chunks"  # Chunks dos pedidos retidos
COLLECTION_PEDIDOS_RETIDOS_TABELA = "pedidos_retidos_tabela"  # Tabela de dados
COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS = "pedidos_retidos_tabela_chunks"  # Chunks da tabela

# ========================================
# SLA
# ========================================
COLLECTION_SLA_GALPAO_ENTRADAS = "galpao_entradas"  # Entradas brutas no galpão (upload)
COLLECTION_SLA_PEDIDOS_GALPAO = "pedidos_no_galpao"  # Pedidos processados no galpão
COLLECTION_SLA_BASES = "sla_bases_data"  # Dados das bases processadas
COLLECTION_SLA_FILES = "sla_files"  # Arquivos SLA processados
COLLECTION_SLA_CHUNKS = "sla_chunks"  # Chunks dos arquivos SLA

# ========================================
# D-1
# ========================================
COLLECTION_D1_MAIN = "d1_main"  # Documento principal/metadados
COLLECTION_D1_CHUNKS = "d1_chunks"  # Chunks dos dados D-1
COLLECTION_D1_BIPAGENS = "d1_bipagens"  # Dados de bipagens em tempo real processados

# ========================================
# SEM MOVIMENTAÇÃO SC
# ========================================
COLLECTION_SEM_MOVIMENTACAO_SC = "sem_movimentacao_sc"  # Documento principal/metadados
COLLECTION_SEM_MOVIMENTACAO_SC_CHUNKS = "sem_movimentacao_sc_chunks"  # Chunks dos dados
