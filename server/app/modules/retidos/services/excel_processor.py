import openpyxl
from io import BytesIO
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class ExcelProcessor:
    def __init__(self):
        self.supported_formats = ['.xlsx', '.xls']
    
    async def process_file(self, file_content: bytes, filename: str) -> tuple[List[Dict[str, Any]], List[str]]:
        """
        Processa arquivo Excel e retorna lista de dicionários normalizados
        """
        try:
            # Verificar formato do arquivo
            if not any(filename.lower().endswith(fmt) for fmt in self.supported_formats):
                raise ValueError(f"Formato não suportado. Use: {', '.join(self.supported_formats)}")
            
            # Ler arquivo Excel
            workbook = openpyxl.load_workbook(BytesIO(file_content))
            sheet = workbook.active
            
            # Obter cabeçalhos da primeira linha
            headers = []
            for cell in sheet[1]:
                header_value = cell.value
                if header_value is None or str(header_value).strip() == '':
                    headers.append(f"col_{len(headers)}")
                else:
                    # Converter para string e limpar
                    headers.append(str(header_value).strip())
            
            # Converter para lista de dicionários
            data = []
            for row in sheet.iter_rows(min_row=2, values_only=True):
                row_dict = {}
                for i, value in enumerate(row):
                    if i < len(headers):
                        # Processar valor da célula
                        processed_value = self._process_cell_value(value)
                        row_dict[headers[i]] = processed_value
                data.append(row_dict)
            
            # Normalizar dados
            normalized_data = []
            for i, item in enumerate(data):
                if i < 3:  # Log apenas os primeiros 3 itens para debug
                    pass
                normalized_item = self._normalize_item(item)
                if normalized_item:  # Só adiciona se não for vazio
                    # Adicionar colunas extras do sistema
                    normalized_item = self._add_system_columns(normalized_item)
                    normalized_data.append(normalized_item)
            
            # Retornar dados e lista de colunas encontradas (incluindo colunas do sistema)
            columns_found = list(headers) if headers else []
            # Adicionar as colunas do sistema à lista
            columns_found.extend(["TELEFONE_MOTORISTA", "STATUS_PROCESSAMENTO"])
            
            return normalized_data, columns_found
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {filename}: {str(e)}")
            raise Exception(f"Erro ao processar arquivo: {str(e)}")
    
    def _process_cell_value(self, value) -> str:
        """
        Processa o valor de uma célula do Excel
        """
        try:
            if value is None:
                return ""
            
            # Se for um número, converter para string sem formatação
            if isinstance(value, (int, float)):
                # Para números muito grandes (como números de pedido), manter como string
                if isinstance(value, int) and value > 999999999:
                    return str(int(value))
                return str(value)
            
            # Se for datetime, converter para string
            if hasattr(value, 'strftime'):
                return value.strftime('%Y-%m-%d %H:%M:%S')
            
            # Para outros tipos, converter para string
            return str(value).strip()
        except Exception as e:
            logger.error(f"Erro ao processar valor da célula: {str(e)}")
            return str(value) if value is not None else ""
    
    def _normalize_item(self, item: Dict[str, Any]) -> Dict[str, str]:
        """
        Processa um item do Excel mantendo TODAS as colunas originais
        """
        try:
            normalized_item = {}
            
            # Processar cada coluna do item original
            for key, value in item.items():
                if value is not None:
                    # Converter para string e limpar espaços
                    clean_value = str(value).strip()
                    if clean_value:  # Só adiciona se não estiver vazio
                        normalized_item[key] = clean_value
            
            # Retorna o item se tiver pelo menos uma coluna com dados
            if normalized_item:
                return normalized_item
            else:
                return None
        except Exception as e:
            logger.error(f"Erro ao processar item: {str(e)}")
            return None
    
    def _add_system_columns(self, item: Dict[str, str]) -> Dict[str, str]:
        """
        Adiciona colunas extras do sistema ao item processado
        """
        try:
            # Adicionar as 2 colunas extras do sistema
            item["TELEFONE_MOTORISTA"] = ""  # Coluna 1: Telefone do motorista
            item["STATUS_PROCESSAMENTO"] = "PENDENTE"  # Coluna 2: Status do processamento
            
            # Extrair base da coluna "Unidade responsável" se existir
            unidade_responsavel = item.get("Unidade responsável", "")
            if unidade_responsavel and unidade_responsavel.strip():
                # Usar "Unidade responsável" como BASE se não existir coluna BASE
                if not item.get("BASE") or not item["BASE"].strip():
                    item["BASE"] = unidade_responsavel.strip()
                # Também manter a coluna original
                item["UNIDADE_RESPONSAVEL"] = unidade_responsavel.strip()
            
            return item
        except Exception as e:
            logger.error(f"Erro ao adicionar colunas do sistema: {str(e)}")
            return item