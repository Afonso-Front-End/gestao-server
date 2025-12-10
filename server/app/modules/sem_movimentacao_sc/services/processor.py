"""
Processador para dados de Sem Movimentação SC
"""
import openpyxl
from io import BytesIO
from typing import List, Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class SemMovimentacaoSCProcessor:
    """
    Processador para dados de Sem Movimentação SC
    - Processa arquivo Excel
    - Mapeia colunas para estrutura padronizada
    - Salva no MongoDB
    """
    
    def __init__(self):
        self.supported_formats = ['.xlsx', '.xls']
        
        # Mapeamento de colunas esperadas (com variações possíveis)
        self.column_mapping = {
            'remessa': [
                'Remessa', 'remessa', 'REMESSA',
                'Número da Remessa', 'Numero da Remessa',
                'Nº Remessa', 'N Remessa'
            ],
            'nome_base_mais_recente': [
                'Nome da base mais recente', 'Nome da Base Mais Recente',
                'Base Mais Recente', 'base mais recente',
                'Nome da Base', 'Base'
            ],
            'unidade_responsavel': [
                'Unidade responsável', 'Unidade Responsável',
                'unidade responsavel', 'Unidade Responsavel',
                'Responsável', 'responsavel'
            ],
            'base_entrega': [
                'Base de entrega', 'Base de Entrega',
                'base de entrega', 'Base Entrega',
                'Base Entrega', 'Entrega'
            ],
            'horario_ultima_operacao': [
                'Horário da última operação', 'Horário da Última Operação',
                'horario da ultima operacao', 'Horario da Ultima Operacao',
                'Última Operação', 'Ultima Operacao',
                'Data Última Operação', 'Data Ultima Operacao'
            ],
            'tipo_ultima_operacao': [
                'Tipo da última operação', 'Tipo da Última Operação',
                'tipo da ultima operacao', 'Tipo da Ultima Operacao',
                'Tipo Operação', 'Tipo Operacao',
                'Tipo', 'tipo'
            ],
            'operador_bipe_mais_recente': [
                'Operador do bipe mais recente', 'Operador do Bipe Mais Recente',
                'operador do bipe mais recente', 'Operador do Bipe Mais Recente',
                'Operador Bipe', 'operador bipe',
                'Operador', 'operador'
            ],
            'aging': [
                'Aging', 'aging', 'AGING',
                'Idade', 'idade', 'IDADE'
            ],
            'numero_id': [
                'Número do ID', 'Numero do ID',
                'Número ID', 'Numero ID',
                'Nº ID', 'N ID',
                'ID', 'id', 'Id'
            ]
        }
    
    async def process_file(
        self, 
        file_content: bytes, 
        filename: str
    ) -> Dict[str, Any]:
        """
        Processa arquivo Excel de Sem Movimentação SC
        
        Args:
            file_content: Conteúdo do arquivo em bytes
            filename: Nome do arquivo
            
        Returns:
            Dict com resultado do processamento
        """
        try:
            # Verificar formato
            if not any(filename.lower().endswith(fmt) for fmt in self.supported_formats):
                raise ValueError(f"Formato não suportado. Use: {', '.join(self.supported_formats)}")
            
            logger.info(f"📊 Iniciando processamento de {filename}")
            
            # Ler Excel
            workbook = openpyxl.load_workbook(BytesIO(file_content), data_only=True)
            sheet = workbook.active
            
            # Ler cabeçalhos
            headers = []
            for cell in sheet[1]:
                header_value = cell.value
                if header_value:
                    headers.append(str(header_value).strip())
                else:
                    headers.append('')
            
            logger.info(f"📋 Cabeçalhos encontrados: {len(headers)} colunas")
            logger.info(f"   Cabeçalhos: {headers[:10]}...")  # Mostrar primeiros 10
            
            # Mapear índices das colunas
            column_indices = self._map_columns(headers)
            
            # Verificar se todas as colunas obrigatórias foram encontradas
            missing_columns = [key for key, idx in column_indices.items() if idx is None]
            if missing_columns:
                logger.warning(f"⚠️ Colunas não encontradas: {missing_columns}")
                logger.info(f"   Tentando mapear com variações...")
            
            # Processar linhas
            dados_processados = []
            total_rows = 0
            linhas_vazias = 0
            
            for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
                total_rows += 1
                
                # Verificar se a linha está vazia
                if not any(cell for cell in row if cell is not None):
                    linhas_vazias += 1
                    continue
                
                # Mapear dados da linha
                registro = self._map_row_data(row, column_indices, headers)
                
                if registro:
                    # Adicionar metadados
                    registro['_processado_em'] = datetime.now()
                    registro['_arquivo_origem'] = filename
                    dados_processados.append(registro)
            
            logger.info(f"✅ Processamento concluído:")
            logger.info(f"   Total de linhas processadas: {total_rows}")
            logger.info(f"   Linhas vazias ignoradas: {linhas_vazias}")
            logger.info(f"   Registros válidos: {len(dados_processados)}")
            
            workbook.close()
            
            return {
                "success": True,
                "total_rows": total_rows,
                "total_valid": len(dados_processados),
                "total_empty": linhas_vazias,
                "columns_found": headers,
                "columns_mapped": {k: headers[v] if v is not None else None 
                                  for k, v in column_indices.items()},
                "data": dados_processados
            }
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar arquivo: {str(e)}", exc_info=True)
            raise
    
    def _map_columns(self, headers: List[str]) -> Dict[str, int]:
        """
        Mapeia os cabeçalhos para os índices das colunas esperadas
        
        Args:
            headers: Lista de cabeçalhos do Excel
            
        Returns:
            Dict com {nome_campo: indice_coluna} ou None se não encontrado
        """
        column_indices = {}
        
        for field_name, possible_names in self.column_mapping.items():
            column_indices[field_name] = None
            
            # Procurar por cada variação possível
            for idx, header in enumerate(headers):
                header_clean = str(header).strip() if header else ''
                
                # Verificar correspondência exata ou parcial
                for possible_name in possible_names:
                    if header_clean.lower() == possible_name.lower():
                        column_indices[field_name] = idx
                        logger.info(f"   ✓ Mapeado '{field_name}' -> coluna {idx}: '{header_clean}'")
                        break
                
                if column_indices[field_name] is not None:
                    break
        
        return column_indices
    
    def _map_row_data(
        self, 
        row: tuple, 
        column_indices: Dict[str, int],
        headers: List[str]
    ) -> Dict[str, Any]:
        """
        Mapeia os dados de uma linha para o formato padronizado
        
        A ordem dos campos no dict será a ordem especificada:
        1. Remessa
        2. Nome da base mais recente
        3. Unidade responsável
        4. Base de entrega
        5. Horário da última operação
        6. Tipo da última operação
        7. Operador do bipe mais recente
        8. Aging
        9. Número do ID
        
        Args:
            row: Tupla com os valores da linha
            column_indices: Dict com índices das colunas mapeadas
            headers: Lista de cabeçalhos (para debug)
            
        Returns:
            Dict com os dados mapeados na ordem correta ou None se linha inválida
        """
        # Definir ordem dos campos (conforme especificado)
        field_order = [
            'remessa',
            'nome_base_mais_recente',
            'unidade_responsavel',
            'base_entrega',
            'horario_ultima_operacao',
            'tipo_ultima_operacao',
            'operador_bipe_mais_recente',
            'aging',
            'numero_id'
        ]
        
        registro = {}
        
        # Mapear cada campo na ordem especificada
        for field_name in field_order:
            col_idx = column_indices.get(field_name)
            
            if col_idx is not None and col_idx < len(row):
                value = row[col_idx]
                
                # Processar valor baseado no tipo
                if value is None:
                    registro[field_name] = None
                elif isinstance(value, datetime):
                    registro[field_name] = value.isoformat()
                elif isinstance(value, (int, float)):
                    # Converter para string se for número muito grande (IDs)
                    if field_name == 'numero_id':
                        registro[field_name] = str(value)
                    else:
                        registro[field_name] = value
                else:
                    registro[field_name] = str(value).strip() if str(value).strip() else None
            else:
                registro[field_name] = None
        
        # Verificar se pelo menos um campo obrigatório tem valor
        # Remessa e Número do ID são os mais importantes
        if not registro.get('remessa') and not registro.get('numero_id'):
            return None
        
        return registro

