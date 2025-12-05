"""
Serviço para criar snapshots de dados para reports
"""
import logging
from datetime import datetime
from typing import Dict, Any, List
from app.services.database import get_database
from app.core.collections import (
    COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS,
    COLLECTION_PEDIDOS_RETIDOS_CHUNKS,
    COLLECTION_D1_BIPAGENS
)

logger = logging.getLogger(__name__)


class SnapshotService:
    """Serviço para criar e gerenciar snapshots"""
    
    @staticmethod
    def _normalize_string(s: str) -> str:
        """Normaliza string para comparação"""
        if not s:
            return ""
        return s.strip().upper()
    
    @staticmethod
    def _get_responsavel(item: dict) -> str:
        """Extrai responsável do item"""
        return (
            item.get("Responsável pela entrega") or 
            item.get("Responsável") or 
            item.get("RESPONSAVEL") or 
            item.get("responsavel") or 
            ""
        )
    
    @staticmethod
    def _get_base(item: dict) -> str:
        """Extrai base do item"""
        return item.get("Base de entrega") or item.get("BASE DE ENTREGA") or item.get("base") or ""
    
    @staticmethod
    def _get_cidade(item: dict) -> str:
        """Extrai cidade do item"""
        return item.get("Cidade Destino") or item.get("CIDADE DESTINO") or item.get("cidade") or ""
    
    @staticmethod
    def _get_tipo_operacao(item: dict) -> str:
        """Extrai tipo de operação do item"""
        return (
            item.get("Tipo de correio") or 
            item.get("Tipo de operação") or 
            item.get("TIPO DE OPERACAO") or 
            item.get("tipo") or 
            ""
        )
    
    @staticmethod
    def _calcular_aging(item: dict) -> str:
        """
        Calcula aging em dias baseado na diferença entre data atual e data de saída para entrega
        Retorna categoria: "0-3 dias", "4-7 dias", "8-14 dias", "15+ dias"
        """
        try:
            # Tentar pegar a data de saída para entrega (mais recente = mais preciso)
            data_criacao_str = (
                item.get("Horário de saída para entrega") or
                item.get("Tempo de entrega") or
                item.get("Tempo de atualização") or
                item.get("Data de criação") or
                ""
            )
            
            if not data_criacao_str:
                return "Sem data"
            
            # Parse da data (formato esperado: "2025-09-23 12:47:00")
            if isinstance(data_criacao_str, str):
                # Tentar vários formatos comuns
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d/%m/%Y %H:%M:%S"]:
                    try:
                        data_criacao = datetime.strptime(data_criacao_str, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    # Se nenhum formato funcionou
                    return "Formato inválido"
            else:
                data_criacao = data_criacao_str
            
            # Calcular diferença em dias
            hoje = datetime.now()
            diferenca = (hoje - data_criacao).days
            
            # Categorizar (pedidos parados - faixas menores fazem mais sentido)
            if diferenca < 0:
                return "Data futura"
            elif diferenca <= 3:
                return "0-3 dias"
            elif diferenca <= 7:
                return "4-7 dias"
            elif diferenca <= 14:
                return "8-14 dias"
            else:
                return "15+ dias"
                
        except Exception as e:
            logger.debug(f"Erro ao calcular aging: {e}")
            return "Erro no cálculo"
    
    @staticmethod
    def _is_entregue(item: dict) -> bool:
        """Verifica se pedido foi entregue"""
        marca = SnapshotService._normalize_string(item.get("Marca de assinatura", ""))
        return "RECEBIMENTO COM ASSINATURA NORMAL" in marca or "ASSINATURA DE DEVOLUÇÃO" in marca
    
    @staticmethod
    async def create_pedidos_parados_snapshot() -> Dict[str, Any]:
        """
        Cria snapshot com métricas dos pedidos parados
        """
        try:
            db = get_database()
            if db is None:
                raise Exception("Database não conectado")
            
            collection = db[COLLECTION_PEDIDOS_RETIDOS_TABELA_CHUNKS]
            
            # Buscar todos os dados
            cursor = collection.find({}).sort("chunk_number", 1)
            
            # Conjuntos para contar únicos
            motoristas_set = set()
            bases_set = set()
            cidades_set = set()
            
            # Contadores
            total_pedidos = 0
            entregues = 0
            nao_entregues = 0
            
            # Distribuições
            por_base: Dict[str, Dict] = {}
            por_cidade: Dict[str, Dict] = {}
            por_motorista: Dict[str, Dict] = {}
            por_aging: Dict[str, int] = {}
            
            # Status de contato (buscar da coleção motoristas_status_pedidos_retidos)
            status_collection = db["motoristas_status_pedidos_retidos"]
            status_cursor = status_collection.find({})
            contatos = {
                "retornou": 0,
                "nao_retornou": 0,
                "esperando_retorno": 0,
                "numero_errado": 0
            }
            
            async for status_doc in status_cursor:
                status = status_doc.get("status", "")
                if status == "Retornou":
                    contatos["retornou"] += 1
                elif status == "Não retornou":
                    contatos["nao_retornou"] += 1
                elif status == "Esperando retorno":
                    contatos["esperando_retorno"] += 1
                elif status == "Número de contato errado":
                    contatos["numero_errado"] += 1
            
            # Processar chunks
            async for chunk in cursor:
                chunk_data = chunk.get("data", []) or []
                
                for item in chunk_data:
                    total_pedidos += 1
                    
                    # Extrair informações
                    motorista = SnapshotService._get_responsavel(item)
                    base = SnapshotService._get_base(item)
                    cidade = SnapshotService._get_cidade(item)
                    aging = SnapshotService._calcular_aging(item)
                    is_entregue = SnapshotService._is_entregue(item)
                    
                    # Adicionar aos sets
                    if motorista:
                        motoristas_set.add(motorista)
                    if base:
                        bases_set.add(base)
                    if cidade:
                        cidades_set.add(cidade)
                    
                    # Contar entregues/não entregues
                    if is_entregue:
                        entregues += 1
                    else:
                        nao_entregues += 1
                    
                    # Distribuição por base
                    if base:
                        if base not in por_base:
                            por_base[base] = {"total": 0, "entregues": 0, "nao_entregues": 0}
                        por_base[base]["total"] += 1
                        if is_entregue:
                            por_base[base]["entregues"] += 1
                        else:
                            por_base[base]["nao_entregues"] += 1
                    
                    # Distribuição por cidade
                    if cidade:
                        if cidade not in por_cidade:
                            por_cidade[cidade] = {"total": 0, "entregues": 0, "nao_entregues": 0}
                        por_cidade[cidade]["total"] += 1
                        if is_entregue:
                            por_cidade[cidade]["entregues"] += 1
                        else:
                            por_cidade[cidade]["nao_entregues"] += 1
                    
                    # Distribuição por motorista
                    if motorista:
                        if motorista not in por_motorista:
                            por_motorista[motorista] = {"total": 0, "entregues": 0, "nao_entregues": 0}
                        por_motorista[motorista]["total"] += 1
                        if is_entregue:
                            por_motorista[motorista]["entregues"] += 1
                        else:
                            por_motorista[motorista]["nao_entregues"] += 1
                    
                    # Distribuição por aging
                    if aging:
                        por_aging[aging] = por_aging.get(aging, 0) + 1
            
            # Calcular taxa de entrega
            taxa_entrega = (entregues / total_pedidos * 100) if total_pedidos > 0 else 0.0
            
            # Formatar distribuições
            bases_list = [
                {
                    "base": base,
                    "total": data["total"],
                    "entregues": data["entregues"],
                    "nao_entregues": data["nao_entregues"],
                    "taxa_entrega": round((data["entregues"] / data["total"] * 100) if data["total"] > 0 else 0.0, 2)
                }
                for base, data in por_base.items()
            ]
            bases_list.sort(key=lambda x: x["total"], reverse=True)
            
            # Top 20 cidades
            cidades_list = [
                {
                    "cidade": cidade,
                    "total": data["total"],
                    "entregues": data["entregues"],
                    "nao_entregues": data["nao_entregues"],
                    "taxa_entrega": round((data["entregues"] / data["total"] * 100) if data["total"] > 0 else 0.0, 2)
                }
                for cidade, data in por_cidade.items()
            ]
            cidades_list.sort(key=lambda x: x["total"], reverse=True)
            top_cidades = cidades_list[:20]  # Top 20 cidades
            
            # Top 10 motoristas
            motoristas_list = [
                {
                    "motorista": mot,
                    "total": data["total"],
                    "entregues": data["entregues"],
                    "nao_entregues": data["nao_entregues"],
                    "taxa_entrega": round((data["entregues"] / data["total"] * 100) if data["total"] > 0 else 0.0, 2)
                }
                for mot, data in por_motorista.items()
            ]
            motoristas_list.sort(key=lambda x: x["total"], reverse=True)
            top_motoristas = motoristas_list[:10]
            
            # Aging list - ordenar por categoria (não por quantidade)
            aging_ordem = [
                "0-3 dias", "4-7 dias", "8-14 dias", "15+ dias",
                "Sem data", "Data futura", "Erro no cálculo"
            ]
            aging_list = []
            for aging_cat in aging_ordem:
                if aging_cat in por_aging:
                    aging_list.append({
                        "aging": aging_cat,
                        "total": por_aging[aging_cat]
                    })
            
            # Adicionar qualquer categoria não esperada no final
            for aging, total in por_aging.items():
                if aging not in aging_ordem:
                    aging_list.append({"aging": aging, "total": total})
            
            # Montar snapshot
            snapshot = {
                "snapshot_date": datetime.now(),
                "module": "pedidos_parados",
                "period_type": "manual",
                "metrics": {
                    "total_pedidos": total_pedidos,
                    "total_motoristas": len(motoristas_set),
                    "total_bases": len(bases_set),
                    "total_cidades": len(cidades_set),
                    "entregues": entregues,
                    "nao_entregues": nao_entregues,
                    "taxa_entrega": round(taxa_entrega, 2),
                    "contatos": contatos,
                    "por_base": bases_list,
                    "top_cidades": top_cidades,
                    "top_motoristas": top_motoristas,
                    "por_aging": aging_list
                },
                "created_by": "manual",
                "created_at": datetime.now()
            }
            
            # Salvar snapshot na coleção
            snapshots_collection = db["reports_snapshots"]
            result = await snapshots_collection.insert_one(snapshot)
            
            logger.info(f"✅ Snapshot criado com sucesso: {result.inserted_id}")
            
            return {
                "success": True,
                "snapshot_id": str(result.inserted_id),
                "metrics": snapshot["metrics"]
            }
            
        except Exception as e:
            logger.error(f"❌ Erro ao criar snapshot: {str(e)}")
            raise
    
    @staticmethod
    async def create_d1_snapshot() -> Dict[str, Any]:
        """
        Cria snapshot com métricas dos dados D1 (bipagens)
        """
        try:
            db = get_database()
            if db is None:
                raise Exception("Database não conectado")
            
            collection = db[COLLECTION_D1_BIPAGENS]
            
            # Pipeline para pegar apenas a bipagem mais recente de cada pedido
            pipeline = [
                {'$sort': {
                    'numero_pedido_jms': 1,
                    'tempo_digitalizacao': -1
                }},
                {'$group': {
                    '_id': '$numero_pedido_jms',
                    'doc': {'$first': '$$ROOT'}
                }},
                {'$replaceRoot': {'newRoot': '$doc'}},
                {'$match': {
                    'responsavel_entrega': {'$exists': True, '$ne': '', '$ne': None},
                    'esta_com_motorista': True
                }}
            ]
            
            # Conjuntos para contar únicos
            motoristas_set = set()
            bases_set = set()
            cidades_set = set()
            
            # Contadores
            total_pedidos = 0
            entregues = 0
            nao_entregues = 0
            
            # Distribuições
            por_base: Dict[str, Dict] = {}
            por_cidade: Dict[str, Dict] = {}
            por_motorista: Dict[str, Dict] = {}
            por_tempo_parado: Dict[str, int] = {}
            
            # Status de contato (buscar da coleção motoristas_status_d1)
            status_collection = db["motoristas_status_d1"]
            status_cursor = status_collection.find({})
            contatos = {
                "retornou": 0,
                "nao_retornou": 0,
                "esperando_retorno": 0,
                "numero_errado": 0
            }
            
            async for status_doc in status_cursor:
                status = status_doc.get("status", "")
                if status == "Retornou":
                    contatos["retornou"] += 1
                elif status == "Não retornou":
                    contatos["nao_retornou"] += 1
                elif status == "Esperando retorno":
                    contatos["esperando_retorno"] += 1
                elif status == "Número de contato errado":
                    contatos["numero_errado"] += 1
            
            # Processar dados
            async for doc in collection.aggregate(pipeline):
                total_pedidos += 1
                
                # Extrair informações
                motorista = doc.get('responsavel_entrega', '')
                base = doc.get('base_entrega', '') or doc.get('base_escaneamento', '')
                cidade = doc.get('cidade_destino', '')
                tempo_parado = doc.get('tempo_pedido_parado', 'Sem tempo')
                marca = doc.get('marca_assinatura', '').lower()
                is_entregue = "recebimento com assinatura normal" in marca or "assinatura de devolução" in marca or marca == "entregue"
                
                # Adicionar aos sets
                if motorista:
                    motoristas_set.add(motorista)
                if base:
                    bases_set.add(base)
                if cidade:
                    cidades_set.add(cidade)
                
                # Contar entregues/não entregues
                if is_entregue:
                    entregues += 1
                else:
                    nao_entregues += 1
                
                # Distribuição por base
                if base:
                    if base not in por_base:
                        por_base[base] = {"total": 0, "entregues": 0, "nao_entregues": 0}
                    por_base[base]["total"] += 1
                    if is_entregue:
                        por_base[base]["entregues"] += 1
                    else:
                        por_base[base]["nao_entregues"] += 1
                
                # Distribuição por cidade
                if cidade:
                    if cidade not in por_cidade:
                        por_cidade[cidade] = {"total": 0, "entregues": 0, "nao_entregues": 0}
                    por_cidade[cidade]["total"] += 1
                    if is_entregue:
                        por_cidade[cidade]["entregues"] += 1
                    else:
                        por_cidade[cidade]["nao_entregues"] += 1
                
                # Distribuição por motorista
                if motorista:
                    if motorista not in por_motorista:
                        por_motorista[motorista] = {"total": 0, "entregues": 0, "nao_entregues": 0}
                    por_motorista[motorista]["total"] += 1
                    if is_entregue:
                        por_motorista[motorista]["entregues"] += 1
                    else:
                        por_motorista[motorista]["nao_entregues"] += 1
                
                # Distribuição por tempo parado
                if tempo_parado:
                    por_tempo_parado[tempo_parado] = por_tempo_parado.get(tempo_parado, 0) + 1
            
            # Calcular taxa de entrega
            taxa_entrega = (entregues / total_pedidos * 100) if total_pedidos > 0 else 0.0
            
            # Formatar distribuições
            bases_list = [
                {
                    "base": base,
                    "total": data["total"],
                    "entregues": data["entregues"],
                    "nao_entregues": data["nao_entregues"],
                    "taxa_entrega": round((data["entregues"] / data["total"] * 100) if data["total"] > 0 else 0.0, 2)
                }
                for base, data in por_base.items()
            ]
            bases_list.sort(key=lambda x: x["total"], reverse=True)
            
            # Top 20 cidades
            cidades_list = [
                {
                    "cidade": cidade,
                    "total": data["total"],
                    "entregues": data["entregues"],
                    "nao_entregues": data["nao_entregues"],
                    "taxa_entrega": round((data["entregues"] / data["total"] * 100) if data["total"] > 0 else 0.0, 2)
                }
                for cidade, data in por_cidade.items()
            ]
            cidades_list.sort(key=lambda x: x["total"], reverse=True)
            top_cidades = cidades_list[:20]
            
            # Top 10 motoristas
            motoristas_list = [
                {
                    "motorista": mot,
                    "total": data["total"],
                    "entregues": data["entregues"],
                    "nao_entregues": data["nao_entregues"],
                    "taxa_entrega": round((data["entregues"] / data["total"] * 100) if data["total"] > 0 else 0.0, 2)
                }
                for mot, data in por_motorista.items()
            ]
            motoristas_list.sort(key=lambda x: x["total"], reverse=True)
            top_motoristas = motoristas_list[:10]
            
            # Tempo parado list
            tempo_parado_list = [
                {"tempo_parado": tempo, "total": total}
                for tempo, total in sorted(por_tempo_parado.items(), key=lambda x: x[1], reverse=True)
            ]
            
            # Montar snapshot
            snapshot = {
                "snapshot_date": datetime.now(),
                "module": "d1",
                "period_type": "manual",
                "metrics": {
                    "total_pedidos": total_pedidos,
                    "total_motoristas": len(motoristas_set),
                    "total_bases": len(bases_set),
                    "total_cidades": len(cidades_set),
                    "entregues": entregues,
                    "nao_entregues": nao_entregues,
                    "taxa_entrega": round(taxa_entrega, 2),
                    "contatos": contatos,
                    "por_base": bases_list,
                    "top_cidades": top_cidades,
                    "top_motoristas": top_motoristas,
                    "por_tempo_parado": tempo_parado_list
                },
                "created_by": "manual",
                "created_at": datetime.now()
            }
            
            # Salvar snapshot na coleção específica para D1
            snapshots_collection = db["d1_reports_snapshots"]
            result = await snapshots_collection.insert_one(snapshot)
            
            logger.info(f"✅ Snapshot D1 criado com sucesso: {result.inserted_id}")
            
            return {
                "success": True,
                "snapshot_id": str(result.inserted_id),
                "metrics": snapshot["metrics"]
            }
            
        except Exception as e:
            logger.error(f"❌ Erro ao criar snapshot D1: {str(e)}")
            raise
    
    @staticmethod
    async def create_sla_snapshot(base: Optional[str] = None, cities: Optional[List[str]] = None, custom_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Cria snapshot com métricas dos dados SLA
        
        Args:
            base: Base específica para criar snapshot (se None, processa todas)
            cities: Lista de cidades para filtrar (se None ou vazio, salva geral da base)
        """
        try:
            db = get_database()
            if db is None:
                raise Exception("Database não conectado")
            
            from app.modules.sla.services.sla_calculator import SLACalculator
            from app.core.collections import COLLECTION_SLA_BASES
            
            sla_calculator = SLACalculator()
            sla_bases_collection = db[COLLECTION_SLA_BASES]
            
            # Se base foi especificada, processar apenas essa base
            if base:
                bases_to_process = [base]
            else:
                # Buscar todas as bases processadas
                bases_cursor = sla_bases_collection.find({})
                bases_to_process = []
                async for base_doc in bases_cursor:
                    base_name = base_doc.get("base_name", "")
                    if base_name:
                        bases_to_process.append(base_name)
            
            # Conjuntos para contar únicos
            motoristas_set = set()
            bases_set = set()
            cidades_set = set()
            
            # Contadores globais
            total_pedidos = 0
            entregues = 0
            nao_entregues = 0
            
            # Distribuições
            por_base: Dict[str, Dict] = {}
            por_cidade: Dict[str, Dict] = {}
            por_motorista: Dict[str, Dict] = {}
            
            # Status de contato (buscar da coleção motorista_status_sla)
            status_collection = db["motorista_status_sla"]
            status_cursor = status_collection.find({})
            contatos = {
                "retornou": 0,
                "nao_retornou": 0,
                "esperando_retorno": 0,
                "numero_errado": 0
            }
            
            async for status_doc in status_cursor:
                status = status_doc.get("status", "")
                if status == "Retornou":
                    contatos["retornou"] += 1
                elif status == "Não retornou":
                    contatos["nao_retornou"] += 1
                elif status == "Esperando retorno":
                    contatos["esperando_retorno"] += 1
                elif status == "Número de contato errado":
                    contatos["numero_errado"] += 1
            
            # Se há base mas não há cities, buscar todas as cidades disponíveis dessa base
            all_cities_for_snapshot = []
            if base and (not cities or len(cities) == 0):
                # Buscar todas as cidades disponíveis da base
                try:
                    all_cities_for_snapshot = await sla_calculator.get_available_cities(base)
                except Exception as e:
                    logger.warning(f"Erro ao buscar cidades da base {base}: {str(e)}")
            
            # Processar cada base
            for base_name in bases_to_process:
                if not base_name:
                    continue
                
                bases_set.add(base_name)
                
                # Calcular métricas para esta base (com filtro de cidades se fornecido)
                try:
                    result = await sla_calculator.calculate_sla_metrics(base_name, cities)
                    if not result.get("success") or "motoristas" not in result:
                        continue
                    
                    motoristas_data = result.get("motoristas", [])
                    totais = result.get("totais", {})
                    
                    base_total = totais.get("totalPedidos", 0)
                    base_entregues = totais.get("entregues", 0)
                    base_nao_entregues = totais.get("naoEntregues", 0)
                    
                    total_pedidos += base_total
                    entregues += base_entregues
                    nao_entregues += base_nao_entregues
                    
                    # Adicionar à distribuição por base
                    por_base[base_name] = {
                        "total": base_total,
                        "entregues": base_entregues,
                        "nao_entregues": base_nao_entregues,
                        "taxa_entrega": round((base_entregues / base_total * 100) if base_total > 0 else 0.0, 2),
                        "total_motoristas": len(motoristas_data)
                    }
                    
                    # Processar motoristas e cidades
                    for motorista_info in motoristas_data:
                        motorista = motorista_info.get("motorista", "")
                        if motorista:
                            motoristas_set.add(motorista)
                            
                            # Distribuição por motorista (acumular de todas as bases)
                            if motorista not in por_motorista:
                                por_motorista[motorista] = {"total": 0, "entregues": 0, "nao_entregues": 0}
                            por_motorista[motorista]["total"] += motorista_info.get("total", 0)
                            por_motorista[motorista]["entregues"] += motorista_info.get("entregues", 0)
                            por_motorista[motorista]["nao_entregues"] += motorista_info.get("naoEntregues", 0)
                        
                        # Processar cidades
                        cidades_motorista = motorista_info.get("todas_cidades", [])
                        for cidade in cidades_motorista:
                            if cidade:
                                cidades_set.add(cidade)
                                
                                # Distribuição por cidade (acumular)
                                if cidade not in por_cidade:
                                    por_cidade[cidade] = {"total": 0, "entregues": 0, "nao_entregues": 0}
                                # Aproximação: distribuir pedidos do motorista entre suas cidades
                                pedidos_por_cidade = motorista_info.get("total", 0) // max(len(cidades_motorista), 1)
                                por_cidade[cidade]["total"] += pedidos_por_cidade
                                
                except Exception as e:
                    logger.warning(f"Erro ao processar base {base_name} para snapshot SLA: {str(e)}")
                    continue
            
            # Calcular taxa de entrega
            taxa_entrega = (entregues / total_pedidos * 100) if total_pedidos > 0 else 0.0
            
            # Formatar distribuições
            bases_list = [
                {
                    "base": base,
                    "total": data["total"],
                    "entregues": data["entregues"],
                    "nao_entregues": data["nao_entregues"],
                    "taxa_entrega": data["taxa_entrega"],
                    "total_motoristas": data.get("total_motoristas", 0)
                }
                for base, data in por_base.items()
            ]
            bases_list.sort(key=lambda x: x["total"], reverse=True)
            
            # Top 20 cidades
            cidades_list = [
                {
                    "cidade": cidade,
                    "total": data["total"],
                    "entregues": data["entregues"],
                    "nao_entregues": data["nao_entregues"],
                    "taxa_entrega": round((data["entregues"] / data["total"] * 100) if data["total"] > 0 else 0.0, 2)
                }
                for cidade, data in por_cidade.items()
            ]
            cidades_list.sort(key=lambda x: x["total"], reverse=True)
            top_cidades = cidades_list[:20]
            
            # Top 10 motoristas
            motoristas_list = [
                {
                    "motorista": mot,
                    "total": data["total"],
                    "entregues": data["entregues"],
                    "nao_entregues": data["nao_entregues"],
                    "taxa_entrega": round((data["entregues"] / data["total"] * 100) if data["total"] > 0 else 0.0, 2)
                }
                for mot, data in por_motorista.items()
            ]
            motoristas_list.sort(key=lambda x: x["total"], reverse=True)
            top_motoristas = motoristas_list[:10]
            
            # Montar snapshot
            # Para cities, se não foram fornecidas mas há base, usar todas as cidades encontradas
            if base and (not cities or len(cities) == 0) and all_cities_for_snapshot:
                cities_sorted = sorted(all_cities_for_snapshot)
            else:
                cities_sorted = sorted(cities) if cities and len(cities) > 0 else []
            
            # Usar data customizada se fornecida, senão usar data atual
            if custom_date:
                try:
                    from datetime import datetime as dt
                    snapshot_date = dt.strptime(custom_date, "%Y-%m-%d")
                except:
                    snapshot_date = datetime.now()
            else:
                snapshot_date = datetime.now()
            
            snapshot = {
                "snapshot_date": snapshot_date,
                "module": "sla",
                "period_type": "manual",
                "base": base,  # Salvar base usada (None se geral)
                "cities": cities_sorted,  # Salvar cidades usadas (ordenadas, [] se geral)
                "metrics": {
                    "total_pedidos": total_pedidos,
                    "total_motoristas": len(motoristas_set),
                    "total_bases": len(bases_set),
                    "total_cidades": len(cidades_set),
                    "entregues": entregues,
                    "nao_entregues": nao_entregues,
                    "taxa_entrega": round(taxa_entrega, 2),
                    "contatos": contatos,
                    "por_base": bases_list,
                    "top_cidades": top_cidades,
                    "top_motoristas": top_motoristas
                },
                "created_by": "manual",
                "created_at": datetime.now()
            }
            
            # Salvar snapshot na coleção específica para SLA
            snapshots_collection = db["sla_reports_snapshots"]
            result = await snapshots_collection.insert_one(snapshot)
            
            logger.info(f"✅ Snapshot SLA criado com sucesso: {result.inserted_id}")
            
            return {
                "success": True,
                "snapshot_id": str(result.inserted_id),
                "metrics": snapshot["metrics"]
            }
            
        except Exception as e:
            logger.error(f"❌ Erro ao criar snapshot SLA: {str(e)}")
            raise

