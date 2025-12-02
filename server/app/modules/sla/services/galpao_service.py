from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
import logging
from app.services.database import db
from app.modules.sla.models.galpao_entradas import GalpaoEntrada, GalpaoEntradaCreate
from app.core.collections import COLLECTION_SLA_GALPAO_ENTRADAS

logger = logging.getLogger(__name__)

class GalpaoService:
    """Serviço para gerenciar entradas no galpão"""
    
    def __init__(self):
        # Colunas que devem ser IGNORADAS (não salvas no banco)
        self.COLUNAS_IGNORAR = {
            "Número do lote",
            "Chip No.",
            "Parada anterior ou próxima",
            "Saída do dia",
            "Quantidade de volumes",
            "Peso",
            "Tipo de peso",
            "Tipo de produto",
            "Modal",
            "Base remetente",
            "Nome do Cliente",
            "Correio de coleta ou entrega",
            "Número de correio de coleta ou entrega",
            "Signatário",
            "Observação",
            "Dispositivo No.",
            "Celular No.",
            "Comprimento",
            "Largura",
            "Altura",
            "Peso volumétrico",
            "CEP de origem",
            "Número do ID",
            "Selo de veículo",
            "Nome da linha",
            "Reserva No,",
            "Tipo problemático",
            "Descrição de Pacote Problemático",
            "Descrição de pacotes não expedidos",
            "Contato da área de agência",
            "Endereço da área de agência",
            "Município de Destino",
            "Estado da cidade de destino",
            "Peso Faturado",
            "Tipo de produto"
        }
    
    def _get_collection(self):
        """Retorna a coleção do galpão"""
        from app.services.database import db
        return db.database[COLLECTION_SLA_GALPAO_ENTRADAS]
    
    async def upload_entradas_excel(self, file_path: str, base_name: str) -> Dict[str, Any]:
        """
        Importa arquivo Excel com entradas no galpão (apenas importação)
        
        Args:
            file_path: Caminho do arquivo Excel
            base_name: Nome da base
            
        Returns:
            Dict com resultado da importação
        """
        try:
            # Ler arquivo Excel
            df = pd.read_excel(file_path)
            available_columns = list(df.columns)
            
            # Filtrar colunas que devem ser IGNORADAS
            colunas_para_processar = [
                col for col in available_columns 
                if col not in self.COLUNAS_IGNORAR
            ]
            
            # Verificar se há colunas para processar
            if len(colunas_para_processar) == 0:
                return {
                    "error": "Todas as colunas foram ignoradas. Nenhuma coluna válida para processar.",
                    "success": False
                }
            
            # Preparar dados para inserção
            dados_para_inserir = []
            
            for index, row in df.iterrows():
                # Extrair apenas colunas não ignoradas
                linha_data = {}
                
                for coluna in colunas_para_processar:
                    valor = row[coluna]
                    if pd.notna(valor) and str(valor).strip():
                        linha_data[coluna] = str(valor).strip()
                    else:
                        linha_data[coluna] = "N/A"
                
                # Adicionar metadados
                linha_data['_linha_original'] = index + 1
                # Usar a base real dos dados, não a base selecionada
                base_real = linha_data.get("Base de escaneamento", base_name)
                linha_data['_base_name'] = base_real
                linha_data['_created_at'] = datetime.utcnow()
                linha_data['_updated_at'] = datetime.utcnow()
                
                dados_para_inserir.append(linha_data)
            
            # Inserir no banco
            if dados_para_inserir:
                collection = self._get_collection()
                
                # NÃO remover dados existentes - salvar TODAS as bases
                logger.info(f"📊 Salvando dados para base: {base_name}")
                logger.info(f"💡 Dados de outras bases serão mantidos")
                
                # Inserir todos os dados sem validação (será feita no cálculo da SLA)
                logger.info(f"📊 Inserindo {len(dados_para_inserir)} registros no banco em chunks...")
                
                # Processar em chunks de 1000 registros
                chunk_size = 1000
                total_inseridos = 0
                
                for i in range(0, len(dados_para_inserir), chunk_size):
                    chunk = dados_para_inserir[i:i + chunk_size]
                    logger.debug(f"📦 Processando chunk {i//chunk_size + 1}/{(len(dados_para_inserir) + chunk_size - 1)//chunk_size} ({len(chunk)} registros)")
                    
                    try:
                        await collection.insert_many(chunk)
                        total_inseridos += len(chunk)
                        logger.debug(f"✅ Chunk inserido com sucesso: {len(chunk)} registros")
                    except Exception as e:
                        logger.error(f"❌ Erro ao inserir chunk: {str(e)}")
                        # Continuar com o próximo chunk mesmo se um falhar
                        continue
                
                logger.info(f"🎉 Processamento concluído: {total_inseridos} registros inseridos")
                
                return {
                    "success": True,
                    "total_entradas": total_inseridos,
                    "message": f"Importadas {total_inseridos} registros no galpão para base {base_name} (processados em chunks)"
                }
            else:
                return {
                    "error": "Nenhum dado válido encontrado para importar",
                    "success": False
                }
            
        except Exception as e:
            return {
                "error": str(e),
                "success": False
            }
    
