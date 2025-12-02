"""
Funções auxiliares compartilhadas entre as rotas de pedidos retidos
Centraliza toda a lógica de normalização de campos e validações
"""
import logging
import re

logger = logging.getLogger(__name__)

# ==================== NORMALIZAÇÃO DE CAMPOS ====================

def get_numero_pedido(item: dict) -> str:
    """Extrai número do pedido com suporte a múltiplos formatos"""
    return (
        item.get("Número de pedido JMS", "")
        or item.get("Nº DO PEDIDO", "")
        or item.get("NUMERO_PEDIDO", "")
        or item.get("Número do pedido", "")
        or item.get("NUMERO_DO_PEDIDO", "")
        or item.get("Pedido", "")
        or item.get("PEDIDO", "")
        or item.get("Remessa", "")
        or item.get("REMESSA", "")
        or item.get("Número", "")
        or item.get("NUMERO", "")
        or item.get("ID", "")
        or item.get("_id", "")
    ).strip()

def get_base_entrega(item: dict) -> str:
    """Extrai base de entrega com suporte a múltiplos formatos"""
    return (
        item.get("Base de entrega", "")
        or item.get("Unidade responsável", "")
        or item.get("BASE", "")
        or item.get("BASE_ENTREGA", "")
        or item.get("Base", "")
    ).strip()

def get_tipo_operacao(item: dict) -> str:
    """Extrai tipo de operação com suporte a múltiplos formatos"""
    return (
        item.get("Tipo da última operação", "")
        or item.get("TIPO_ULTIMA_OPERACAO", "")
        or item.get("Tipo Operacao", "")
        or item.get("Tipo", "")
        or item.get("OPERACAO", "")
        or item.get("Status", "")
        or item.get("STATUS", "")
    ).strip()

def get_aging(item: dict) -> str:
    """Extrai aging com suporte a múltiplos formatos"""
    return (
        item.get("Aging", "")
        or item.get("AGING", "")
        or item.get("Aging (dias)", "")
        or item.get("Aging dias", "")
        or item.get("Dias Aging", "")
        or item.get("Tempo Aging", "")
        or item.get("Idade", "")
    ).strip()

def get_responsavel(item: dict) -> str:
    """Extrai responsável/motorista com suporte a múltiplos formatos"""
    return (
        item.get("Responsável pela entrega", "")
        or item.get("Responsavel pela entrega", "")
        or item.get("Entregador", "")
        or item.get("Motorista", "")
        or item.get("ENTREGADOR", "")
        or item.get("MOTORISTA", "")
    ).strip() or "Não informado"

def get_marca_assinatura(item: dict) -> str:
    """Extrai marca de assinatura/status com suporte a múltiplos formatos"""
    return (
        item.get("Marca de assinatura", "")
        or item.get("Status", "")
        or item.get("Situacao", "")
        or item.get("Situação", "")
    ).strip()

def get_horario_operacao(item: dict) -> str:
    """Extrai horário da última operação com suporte a múltiplos formatos"""
    return (
        item.get("Horário da última operação", "")
        or item.get("HORARIO_ULTIMA_OPERACAO", "")
        or item.get("Data da última operação", "")
        or item.get("Data última operação", "")
        or item.get("Data Operacao", "")
        or item.get("Data", "")
        or item.get("DATA", "")
    ).strip()

def get_pacote_problematico(item: dict) -> str:
    """Extrai nome do pacote problemático com suporte a múltiplos formatos"""
    return (
        item.get("Nome de pacote problemático", "")
        or item.get("NOME_PACOTE_PROBLEMATICO", "")
        or item.get("Pacote problemático", "")
        or item.get("Pacote", "")
        or item.get("PACOTE", "")
        or item.get("Motivos dos pacotes problemáticos", "")
        or item.get("Motivos", "")
        or item.get("MOTIVOS", "")
    ).strip()

def get_cidade_destino(item: dict) -> str:
    """Extrai cidade de destino com suporte a múltiplos formatos"""
    return (
        item.get("Cidade Destino", "")
        or item.get("Cidade destino", "")
        or item.get("CIDADE_DESTINO", "")
        or item.get("Cidade", "")
        or item.get("CIDADE", "")
    ).strip()

# ==================== VALIDAÇÕES ====================

def is_child_pedido(numero: str) -> bool:
    """
    Detecta se o pedido é filho (sub-pedido)
    Padrões: .n, -n, _n, letra final
    Exemplos: 123.1, 456-2, 789_3, 100A
    """
    if not numero:
        return False
    return bool(
        re.search(r"\.\d+$", numero)
        or re.search(r"-\d+$", numero)
        or re.search(r"_\d+$", numero)
        or re.search(r"[A-Za-z]$", numero)
    )

def is_entregue(marca: str) -> bool:
    """Verifica se o pedido foi entregue com sucesso"""
    marca_lower = (marca or "").lower()
    return (
        "recebimento com assinatura normal" in marca_lower
        or "assinatura de devolução" in marca_lower
    )

def is_nao_entregue(marca: str) -> bool:
    """Verifica se o pedido não foi entregue"""
    marca_lower = (marca or "").lower()
    return (
        "não entregue" in marca_lower
        or "nao entregue" in marca_lower
    )

def normalize_string(s: str) -> str:
    """Normaliza string para comparações (lowercase, sem espaços extras)"""
    return str(s or "").strip().casefold()

def extract_raiz_numero(numero: str) -> str:
    """Extrai apenas dígitos do número (raiz) para agrupamento"""
    return re.sub(r"\D", "", str(numero))

# ==================== FILTROS ====================

def matches_filters(item: dict, bases_list: list, tipos_list: list, aging_list: list) -> bool:
    """Verifica se o item corresponde aos filtros aplicados"""
    # Filtro de bases
    if bases_list:
        base = get_base_entrega(item)
        if not any(base == b for b in bases_list):
            return False
    
    # Filtro de tipos de operação
    if tipos_list:
        tipo = get_tipo_operacao(item)
        if not any(tipo == t for t in tipos_list):
            return False
    
    # Filtro de aging
    if aging_list:
        aging = get_aging(item)
        if not any(aging == a for a in aging_list):
            return False
    
    return True

def extract_pedido_columns(item: dict) -> dict | None:
    """Extrai as colunas específicas do pedido normalizadas"""
    try:
        return {
            "Remessa": get_numero_pedido(item),
            "Tipo da última operação": get_tipo_operacao(item),
            "Horário da última operação": get_horario_operacao(item),
            "Aging": get_aging(item),
            "Nome de pacote problemático": get_pacote_problematico(item),
            "Base de entrega": get_base_entrega(item)
        }
    except Exception as e:
        logger.error(f"Erro ao extrair colunas do pedido: {str(e)}")
        return None

# ==================== COMPATIBILIDADE (funções antigas com prefixo _) ====================

def _matches_filters(item, bases_list, tipos_list, aging_list):
    """[DEPRECATED] Use matches_filters() - Mantido para compatibilidade"""
    return matches_filters(item, bases_list, tipos_list, aging_list)

def _extract_pedido_columns(item):
    """[DEPRECATED] Use extract_pedido_columns() - Mantido para compatibilidade"""
    return extract_pedido_columns(item)

