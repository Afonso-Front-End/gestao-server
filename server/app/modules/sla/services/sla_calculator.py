from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from app.services.database import get_database
from app.core.collections import (
    COLLECTION_SLA_GALPAO_ENTRADAS, 
    COLLECTION_SLA_PEDIDOS_GALPAO,
    COLLECTION_SLA_BASES
)

logger = logging.getLogger(__name__)

class SLACalculator:
    def __init__(self):
        pass
    
    def _get_database(self):
        return get_database()
    
    async def _verificar_galpao_log(self, records: List[Dict], base_name: str) -> None:
        """Apenas verifica e mostra no log se os pedidos existem no galpão"""
        # Garantir que datetime está disponível (evitar problemas de escopo)
        from datetime import datetime as dt_datetime
        datetime = dt_datetime
        try:
            logger.debug(f"\n{'='*60}")
            logger.debug(f"🔍 INICIANDO VERIFICAÇÃO DO GALPÃO")
            logger.debug(f"{'='*60}")
            logger.debug(f"📋 Base: {base_name}")
            logger.debug(f"📦 Total de registros SLA para verificar: {len(records)}")
            
            db = self._get_database()
            
            # Buscar entradas do galpão (já validadas no upload)
            logger.debug(f"\n🔎 Buscando entradas do galpão com base: '{base_name}'")
            
            # Buscar usando múltiplos formatos de base (para garantir que encontre)
            import re
            sigla_match = re.search(r'([A-Z]{2,4})', base_name.upper())
            sigla = sigla_match.group(1) if sigla_match else ""
            
            query_base = {
                "$or": [
                    {"_base_name": base_name},
                    {"_base_name": base_name.strip()},
                    {"Base de escaneamento": base_name},
                    {"Base de escaneamento": base_name.strip()},
                    {"Base de entrega": base_name},
                    {"Base de entrega": base_name.strip()},
                ]
            }
            
            # Adicionar busca por sigla se encontrada
            if sigla:
                query_base["$or"].extend([
                    {"_base_name": {"$regex": sigla, "$options": "i"}},
                    {"Base de escaneamento": {"$regex": sigla, "$options": "i"}},
                    {"Base de entrega": {"$regex": sigla, "$options": "i"}},
                ])
            
            entradas_galpao = await db[COLLECTION_SLA_GALPAO_ENTRADAS].find(query_base).to_list(length=None)
            
            logger.debug(f"Total de entradas no galpão encontradas: {len(entradas_galpao)}")
            logger.debug(f"Query usada: {query_base}")
            
            # Debug: mostrar algumas entradas para verificar
            if entradas_galpao:
                logger.debug(f"✅ Primeira entrada encontrada:")
                logger.debug(f"   • _base_name: '{entradas_galpao[0].get('_base_name', 'N/A')}'")
                logger.debug(f"   • Base de escaneamento: '{entradas_galpao[0].get('Base de escaneamento', 'N/A')}'")
                logger.debug(f"   • Base de entrega: '{entradas_galpao[0].get('Base de entrega', 'N/A')}'")
                logger.debug(f"   • Número de pedido JMS: '{entradas_galpao[0].get('Número de pedido JMS', 'N/A')}'")
            else:
                logger.warning(f"⚠️ Nenhuma entrada encontrada para base: {base_name}")
                
                # Verificar se existem entradas no galpão
                total_geral = await db[COLLECTION_SLA_GALPAO_ENTRADAS].count_documents({})
                logger.debug(f"📊 Total de entradas no galpão (todas as bases): {total_geral}")
                
                if total_geral > 0:
                    todas_entradas = await db[COLLECTION_SLA_GALPAO_ENTRADAS].find({}).to_list(length=10)
                    logger.debug(f"🔍 Exemplos de bases encontradas no galpão:")
                    bases_unicas = set()
                    for entrada in todas_entradas:
                        base_exemplo = (
                            entrada.get('_base_name', 'N/A') or 
                            entrada.get('Base de escaneamento', 'N/A') or 
                            entrada.get('Base de entrega', 'N/A')
                        )
                        if base_exemplo != 'N/A':
                            bases_unicas.add(str(base_exemplo))
                    
                    for base_ex in sorted(bases_unicas)[:10]:
                        logger.debug(f"   • '{base_ex}'")
                        
                    logger.debug(f"\n💡 DICA: Verifique se o nome da base no upload corresponde ao nome usado aqui.")
                    logger.debug(f"   Base buscada: '{base_name}'")
                    logger.debug(f"   Bases encontradas no galpão: {sorted(bases_unicas)}")
            
            # Criar set com números de pedidos do galpão (únicos)
            pedidos_galpao = set()
            total_pedidos_galpao = 0
            for entrada in entradas_galpao:
                numero_pedido = entrada.get("Número de pedido JMS", "")
                # Verificar se é string antes de chamar strip()
                if numero_pedido and isinstance(numero_pedido, str) and numero_pedido.strip():
                    pedidos_galpao.add(numero_pedido.strip())
                    total_pedidos_galpao += 1
                elif numero_pedido and not isinstance(numero_pedido, str):
                    # Se for número, converter para string
                    pedidos_galpao.add(str(numero_pedido))
                    total_pedidos_galpao += 1
            
            logger.debug(f"Total de registros no galpão: {total_pedidos_galpao}")
            logger.debug(f"Total de pedidos únicos no galpão: {len(pedidos_galpao)}")
            logger.debug(f"Total de pedidos duplicados no galpão: {total_pedidos_galpao - len(pedidos_galpao)}")
            
            # Verificar quais pedidos SLA existem no galpão
            pedidos_no_galpao = 0
            pedidos_em_processamento = 0
            
            for record in records:
                numero_pedido = record.get("Número de pedido JMS", "")
                # Verificar se é string antes de chamar strip()
                if numero_pedido and isinstance(numero_pedido, str) and numero_pedido.strip():
                    if numero_pedido.strip() in pedidos_galpao:
                        pedidos_no_galpao += 1
                    else:
                        pedidos_em_processamento += 1
                elif numero_pedido and not isinstance(numero_pedido, str):
                    # Se for número, converter para string
                    if str(numero_pedido) in pedidos_galpao:
                        pedidos_no_galpao += 1
                    else:
                        pedidos_em_processamento += 1
            
            # Verificar pedidos do galpão que não estão na SLA
            pedidos_galpao_nao_sla = 0
            for entrada in entradas_galpao:
                numero_pedido = entrada.get("Número de pedido JMS", "")
                # Verificar se é string antes de chamar strip()
                if numero_pedido and isinstance(numero_pedido, str) and numero_pedido.strip():
                    # Verificar se este pedido existe na SLA
                    existe_na_sla = False
                    for record in records:
                        record_numero = record.get("Número de pedido JMS", "")
                        if isinstance(record_numero, str) and record_numero.strip() == numero_pedido.strip():
                            existe_na_sla = True
                            break
                        elif not isinstance(record_numero, str) and str(record_numero) == numero_pedido.strip():
                            existe_na_sla = True
                            break
                    if not existe_na_sla:
                        pedidos_galpao_nao_sla += 1
                elif numero_pedido and not isinstance(numero_pedido, str):
                    # Se for número, converter para string
                    existe_na_sla = False
                    for record in records:
                        record_numero = record.get("Número de pedido JMS", "")
                        if isinstance(record_numero, str) and record_numero.strip() == str(numero_pedido):
                            existe_na_sla = True
                            break
                        elif not isinstance(record_numero, str) and str(record_numero) == str(numero_pedido):
                            existe_na_sla = True
                            break
                    if not existe_na_sla:
                        pedidos_galpao_nao_sla += 1
            
            # VERIFICAÇÃO AVANÇADA: Comparar tempos de entrega
            logger.debug(f"\nVERIFICAÇÃO AVANÇADA DE TEMPOS:")
            pedidos_bipados_volta = 0
            pedidos_no_galpao_tempo = 0
            
            for record in records:
                numero_pedido_sla = record.get("Número de pedido JMS", "")
                horario_saida_sla = record.get("Horário de saída para entrega", "")
                
                # Verificar se é string antes de chamar strip()
                if numero_pedido_sla and isinstance(numero_pedido_sla, str) and numero_pedido_sla.strip() and horario_saida_sla:
                    # Buscar o mesmo pedido no galpão
                    for entrada in entradas_galpao:
                        numero_pedido_galpao = entrada.get("Número de pedido JMS", "")
                        tempo_digitalizacao_galpao = entrada.get("Tempo de digitalização", "")
                        
                        # Verificar tipos antes de comparar
                        if (isinstance(numero_pedido_galpao, str) and numero_pedido_sla.strip() == numero_pedido_galpao.strip() and 
                            tempo_digitalizacao_galpao) or (not isinstance(numero_pedido_galpao, str) and str(numero_pedido_sla) == str(numero_pedido_galpao) and 
                            tempo_digitalizacao_galpao):
                            
                            # Comparar tempos
                            try:
                                tempo_sla = datetime.strptime(horario_saida_sla, "%Y-%m-%d %H:%M:%S")
                                tempo_galpao = datetime.strptime(tempo_digitalizacao_galpao, "%Y-%m-%d %H:%M:%S")
                                
                                if tempo_sla > tempo_galpao:
                                    # SLA tem tempo mais recente = pedido foi bipado de volta
                                    pedidos_bipados_volta += 1
                                    logger.debug(f"PEDIDO BIPADO DE VOLTA: {numero_pedido_sla}")
                                    logger.debug(f"   • Galpão (Tempo de digitalização): {tempo_digitalizacao_galpao}")
                                    logger.debug(f"   • SLA (Horário de saída para entrega): {horario_saida_sla}")
                                    logger.debug(f"   • Status: Pedido voltou para o motorista")
                                elif tempo_galpao > tempo_sla:
                                    # Galpão tem tempo mais recente = pedido está no galpão
                                    pedidos_no_galpao_tempo += 1
                                    logger.debug(f"PEDIDO NO GALPÃO: {numero_pedido_sla}")
                                    logger.debug(f"   • SLA (Horário de saída para entrega): {horario_saida_sla}")
                                    logger.debug(f"   • Galpão (Tempo de digitalização): {tempo_digitalizacao_galpao}")
                                    logger.debug(f"   • Status: Pedido está no galpão")
                                    
                            except ValueError as e:
                                logger.error(f"Erro ao comparar tempos para {numero_pedido_sla}: {e}")
                                continue
            
            logger.debug(f"\nESTATÍSTICAS DE TEMPO:")
            logger.debug(f"   • Pedidos bipados de volta: {pedidos_bipados_volta}")
            logger.debug(f"   • Pedidos no galpão (tempo): {pedidos_no_galpao_tempo}")
            
            # DETALHAR PEDIDOS ENCONTRADOS E MOVER PARA NOVA COLEÇÃO
            logger.debug(f"\nDETALHAMENTO DOS {pedidos_no_galpao} PEDIDOS ENCONTRADOS:")
            pedidos_detalhados = []
            pedidos_para_mover = []
            
            # Criar set com números de pedidos do galpão para busca rápida
            numeros_galpao_set = set()
            entradas_por_numero = {}
            for entrada in entradas_galpao:
                numero_galpao = entrada.get("Número de pedido JMS", "")
                if numero_galpao:
                    numero_str = str(numero_galpao).strip() if isinstance(numero_galpao, str) else str(numero_galpao).strip()
                    if numero_str:
                        numeros_galpao_set.add(numero_str)
                        entradas_por_numero[numero_str] = entrada
            
            logger.debug(f"📊 Total de números únicos no galpão para comparação: {len(numeros_galpao_set)}")
            
            for record in records:
                numero_pedido_sla = record.get("Número de pedido JMS", "")
                numero_pedido_sla_str = ""
                
                # Normalizar número do pedido SLA
                if numero_pedido_sla:
                    if isinstance(numero_pedido_sla, str):
                        numero_pedido_sla_str = numero_pedido_sla.strip()
                    else:
                        numero_pedido_sla_str = str(numero_pedido_sla).strip()
                
                if not numero_pedido_sla_str:
                    continue
                
                # Verificar se pedido existe no galpão
                if numero_pedido_sla_str in numeros_galpao_set:
                    entrada = entradas_por_numero[numero_pedido_sla_str]
                    
                    # Encontrou coincidência - detalhar
                    pedido_detalhado = {
                        "numero_pedido": numero_pedido_sla_str,
                        "motorista_sla": record.get("Responsável pela entrega", "N/A"),
                        "motorista_galpao": entrada.get("Responsável pela entrega", "N/A"),
                        "horario_saida_sla": record.get("Horário de saída para entrega", "N/A"),
                        "tempo_digitalizacao_galpao": entrada.get("Tempo de digitalização", "N/A"),
                        "marca_assinatura": record.get("Marca de assinatura", "N/A"),
                        "cidade_destino": record.get("Cidade Destino", "N/A"),
                        "status": "DESCONHECIDO"
                    }
                    
                    # Determinar status baseado nos tempos
                    tempo_sla = record.get("Horário de saída para entrega", "")
                    tempo_galpao = entrada.get("Tempo de digitalização", "")
                    
                    mover_para_galpao = False  # Flag para decidir se move
                    
                    if tempo_sla and tempo_galpao:
                        try:
                            tempo_sla_dt = datetime.strptime(tempo_sla, "%Y-%m-%d %H:%M:%S")
                            tempo_galpao_dt = datetime.strptime(tempo_galpao, "%Y-%m-%d %H:%M:%S")
                            
                            if tempo_sla_dt > tempo_galpao_dt:
                                pedido_detalhado["status"] = "NA RUA (BIPADO DE VOLTA)"
                                mover_para_galpao = False
                            elif tempo_galpao_dt > tempo_sla_dt:
                                pedido_detalhado["status"] = "NA BASE (GALPÃO)"
                                mover_para_galpao = True
                            else:
                                pedido_detalhado["status"] = "TEMPOS IGUAIS"
                                mover_para_galpao = True  # Se tempos iguais, considerar no galpão
                        except Exception as e:
                            pedido_detalhado["status"] = f"ERRO AO COMPARAR TEMPOS: {str(e)}"
                            # Se não consegue comparar tempo, mas existe no galpão, MOVER
                            mover_para_galpao = True
                    else:
                        # Se não tem tempo para comparar, mas existe no galpão, MOVER
                        pedido_detalhado["status"] = "NO GALPÃO (SEM TEMPO PARA COMPARAR)"
                        mover_para_galpao = True
                    
                    pedidos_detalhados.append(pedido_detalhado)
                    
                    # Se o pedido está no galpão, preparar para mover
                    if mover_para_galpao:
                        # Extrair base de entrega do record com fallbacks
                        base_entrega_record = (
                            record.get("Base de entrega") or 
                            record.get("Base de Entrega") or
                            record.get("BASE") or
                            record.get("Unidade responsável") or
                            base_name
                        )
                        # Garantir que não seja "N/A" ou vazio
                        if not base_entrega_record or base_entrega_record == "N/A" or base_entrega_record.strip() == "":
                            base_entrega_record = base_name
                        
                        # Criar documento completo para nova coleção
                        pedido_galpao = {
                            **record,  # Copiar todos os campos da SLA
                            "Base de entrega": base_entrega_record,  # Garantir base correta
                            "_moved_from_sla": True,
                            "_moved_at": datetime.utcnow(),
                            "_base_name": base_name,
                            "_tipo_bipagem": "na base",
                            "_tipos_pacote_nao_expedido": entrada.get("Tipos de pacote não expedido", "N/A"),
                            "_impossibilidade_chegar": entrada.get("Impossibilidade.de.chegar.no.endereço.informado客户地址无法进入", "N/A"),
                            "_tempo_digitalizacao_galpao": entrada.get("Tempo de digitalização", "N/A"),
                            "_responsavel_galpao": entrada.get("Responsável pela entrega", "N/A")
                        }
                        pedidos_para_mover.append(pedido_galpao)
                        logger.debug(f"✅ Pedido {numero_pedido_sla_str} adicionado para mover para pedidos_no_galpao")
            
            # Mostrar detalhes dos pedidos
            for i, pedido in enumerate(pedidos_detalhados, 1):
                logger.debug(f"\nPEDIDO {i}: {pedido['numero_pedido']}")
                logger.debug(f"   • Motorista SLA: {pedido['motorista_sla']}")
                logger.debug(f"   • Motorista Galpão: {pedido['motorista_galpao']}")
                logger.debug(f"   • Cidade: {pedido['cidade_destino']}")
                logger.debug(f"   • Marca Assinatura: {pedido['marca_assinatura']}")
                logger.debug(f"   • Tempo SLA: {pedido['horario_saida_sla']}")
                logger.debug(f"   • Tempo Galpão: {pedido['tempo_digitalizacao_galpao']}")
                logger.debug(f"   • STATUS: {pedido['status']}")
            
            # Contar status
            na_base = len([p for p in pedidos_detalhados if "NA BASE" in p['status']])
            na_rua = len([p for p in pedidos_detalhados if "NA RUA" in p['status']])
            tempos_iguais = len([p for p in pedidos_detalhados if "TEMPOS IGUAIS" in p['status']])
            erro_tempo = len([p for p in pedidos_detalhados if "ERRO" in p['status']])
            
            logger.debug(f"\nRESUMO DOS PEDIDOS ENCONTRADOS:")
            logger.debug(f"   • Na Base (Galpão): {na_base}")
            logger.debug(f"   • Na Rua (Bipado de volta): {na_rua}")
            logger.debug(f"   • Tempos iguais: {tempos_iguais}")
            logger.debug(f"   • Erro ao comparar: {erro_tempo}")
            
            # MOVER PEDIDOS PARA NOVA COLEÇÃO COM VALIDAÇÃO DE DUPLICATAS
            logger.debug(f"\n{'='*60}")
            logger.debug(f"📦 RESULTADO DA VERIFICAÇÃO")
            logger.debug(f"{'='*60}")
            logger.debug(f"   • Pedidos encontrados para mover: {len(pedidos_para_mover)}")
            logger.debug(f"   • Pedidos detalhados: {len(pedidos_detalhados)}")
            
            if pedidos_para_mover:
                logger.info(f"\n🚚 MOVENDO {len(pedidos_para_mover)} PEDIDOS PARA COLEÇÃO 'pedidos_no_galpao'...")
                try:
                    # Verificar pedidos já existentes na coleção
                    pedidos_existentes = await db[COLLECTION_SLA_PEDIDOS_GALPAO].find({
                        "_base_name": base_name
                    }).to_list(length=None)
                    
                    # Criar set com números de pedidos já existentes
                    pedidos_ja_existem = set()
                    for pedido_existente in pedidos_existentes:
                        numero_pedido = pedido_existente.get("Número de pedido JMS", "")
                        if numero_pedido:
                            # Verificar se é string antes de chamar strip()
                            if isinstance(numero_pedido, str):
                                pedidos_ja_existem.add(numero_pedido.strip())
                            else:
                                pedidos_ja_existem.add(str(numero_pedido))
                    
                    # Filtrar apenas pedidos que NÃO existem
                    pedidos_novos = []
                    pedidos_duplicados = 0
                    
                    for pedido in pedidos_para_mover:
                        numero_pedido = pedido.get("Número de pedido JMS", "")
                        # Verificar se é string antes de chamar strip()
                        if numero_pedido:
                            if isinstance(numero_pedido, str) and numero_pedido.strip() not in pedidos_ja_existem:
                                pedidos_novos.append(pedido)
                            elif not isinstance(numero_pedido, str) and str(numero_pedido) not in pedidos_ja_existem:
                                pedidos_novos.append(pedido)
                            else:
                                pedidos_duplicados += 1
                                logger.debug(f"Pedido já existe na coleção: {numero_pedido}")
                    
                    if pedidos_novos:
                        # Inserir apenas pedidos novos
                        resultado_insert = await db[COLLECTION_SLA_PEDIDOS_GALPAO].insert_many(pedidos_novos)
                        logger.info(f"✅ SUCESSO: {len(resultado_insert.inserted_ids)} pedidos NOVOS inseridos em 'pedidos_no_galpao'")
                        logger.info(f"   • IDs inseridos: {len(resultado_insert.inserted_ids)}")
                        logger.info(f"   • {pedidos_duplicados} pedidos já existiam (ignorados)")
                        
                        # Verificar se foram realmente inseridos
                        total_na_colecao = await db[COLLECTION_SLA_PEDIDOS_GALPAO].count_documents({"$or": [
                            {"_base_name": base_name},
                            {"Base de entrega": base_name}
                        ]})
                        logger.info(f"   • Total na coleção 'pedidos_no_galpao' para esta base: {total_na_colecao}")
                    else:
                        logger.info(f"Todos os {len(pedidos_para_mover)} pedidos já existem na coleção")
                    
                    # MARCAR PEDIDOS COMO MOVIDOS NA SLA (ao invés de excluir)
                    if pedidos_novos and len(pedidos_novos) > 0:
                        logger.info(f"\nMARCANDO PEDIDOS COMO MOVIDOS NA SLA...")
                        for pedido in pedidos_novos:
                            numero_pedido = pedido.get("Número de pedido JMS", "")
                            if numero_pedido:
                                # Adicionar campo de status na SLA
                                await db[COLLECTION_SLA_BASES].update_one(
                                    {
                                        "base_name": base_name,
                                        "data.Número de pedido JMS": numero_pedido
                                    },
                                    {
                                        "$set": {
                                            "data.$.status_galpao": "movido_para_galpao",
                                            "data.$.moved_at": datetime.utcnow(),
                                            "data.$.tipo_bipagem": "na base"
                                        }
                                    }
                                )
                        logger.info(f"{len(pedidos_novos)} pedidos marcados como movidos na SLA")
                    
                except Exception as e:
                    logger.error(f"Erro ao mover pedidos: {str(e)}")
            else:
                logger.warning(f"\n⚠️ NENHUM PEDIDO PARA MOVER")
                logger.warning(f"   • Motivos possíveis:")
                logger.warning(f"     - Não há correspondências entre SLA e galpão")
                logger.warning(f"     - Todos os pedidos foram bipados de volta (tempo SLA > tempo galpão)")
                logger.warning(f"     - Nenhuma entrada encontrada no galpão para esta base")
                logger.debug(f"\n   • Dados para debug:")
                logger.debug(f"     - Total de registros SLA: {len(records)}")
                logger.debug(f"     - Total de entradas no galpão: {len(entradas_galpao)}")
                logger.debug(f"     - Pedidos no galpão (simples): {pedidos_no_galpao}")
            
            logger.info(f"\nPedidos SLA que EXISTEM no galpão: {pedidos_no_galpao}")
            logger.info(f"Pedidos SLA que NÃO existem no galpão: {pedidos_em_processamento}")
            logger.info(f"Pedidos do galpão que NÃO estão na SLA: {pedidos_galpao_nao_sla}")
            logger.info(f"RESUMO GERAL:")
            logger.info(f"   • Total SLA: {len(records)}")
            logger.info(f"   • Total Galpão: {len(entradas_galpao)}")
            logger.info(f"   • Pedidos únicos no galpão: {len(pedidos_galpao)}")
            logger.info(f"   • Coincidências: {pedidos_no_galpao}")
            logger.info(f"VERIFICAÇÃO CONCLUÍDA - Base: {base_name}")
            logger.debug("=" * 50)
            
        except Exception as e:
            import traceback
            logger.error(f"Erro na verificação do galpão: {str(e)}")
            logger.error(f"Traceback completo: {traceback.format_exc()}")
    
    async def calculate_sla_metrics(self, base_name: str, cities: Optional[List[str]] = None) -> Dict[str, Any]:
        """Retorna dados básicos sem cálculos complexos"""
        try:
            db = self._get_database()
            
            # Buscar a base com múltiplos formatos (similar à busca do galpão)
            import re
            sigla_match = re.search(r'([A-Z]{2,4})', base_name.upper())
            sigla = sigla_match.group(1) if sigla_match else ""
            
            # Tentar busca exata primeiro
            base_doc = await db[COLLECTION_SLA_BASES].find_one({"base_name": base_name})
            
            # Se não encontrar, tentar busca flexível
            if not base_doc:
                query = {
                    "$or": [
                        {"base_name": base_name.strip()},
                        {"base_name": {"$regex": re.escape(base_name.strip()), "$options": "i"}},
                    ]
                }
                
                # Adicionar busca por sigla se encontrada
                if sigla:
                    query["$or"].extend([
                        {"base_name": {"$regex": sigla, "$options": "i"}},
                    ])
                
                # Tentar encontrar qualquer base que contenha partes do nome
                base_doc = await db[COLLECTION_SLA_BASES].find_one(query)
                
                # Se ainda não encontrar, buscar todas e fazer matching manual
                if not base_doc:
                    all_bases = await db[COLLECTION_SLA_BASES].find({}).to_list(length=None)
                    base_normalized = base_name.strip().upper()
                    for base in all_bases:
                        base_db_name = base.get("base_name", "").upper().strip()
                        # Verificar se é exatamente igual ou contém a sigla
                        if base_normalized == base_db_name or (sigla and sigla in base_db_name):
                            base_doc = base
                            break
            
            if not base_doc or "data" not in base_doc:
                return {
                    "success": False,
                    "error": f"Nenhum registro encontrado para a base especificada: '{base_name}'"
                }
            
            # Extrair registros
            records = base_doc["data"]
            
            # Filtrar pedidos movidos para o galpão (não incluir nos cálculos SLA)
            records = [record for record in records if record.get("status_galpao") != "movido_para_galpao"]
            
            # Filtrar por cidades se especificado
            if cities:
                records = [record for record in records if record.get("Cidade Destino") in cities]
            
            if not records:
                return {
                    "success": False,
                    "error": "Nenhum registro encontrado para a base especificada"
                }
            
            # Verificar galpão (apenas para log)
            await self._verificar_galpao_log(records, base_name)
            
            # Pré-carregar pedidos no galpão para excluir do cálculo de "não entregues"
            # IMPORTANTE: pedidos estão na coleção "pedidos_no_galpao", não "galpao_entradas"
            pedidos_no_galpao: set[str] = set()
            try:
                import re
                sigla_match = re.search(r'([A-Z]{2,4})', base_name.upper())
                sigla = sigla_match.group(1) if sigla_match else ""
                
                # Buscar na coleção correta: pedidos_no_galpao
                # Tentar múltiplos formatos de busca por base
                query_base = {
                    "$or": [
                        {"Base de entrega": base_name},
                        {"Base de entrega": base_name.strip()},
                        {"_base_name": base_name},
                        {"_base_name": base_name.strip()},
                        {"Base de entrega": {"$regex": sigla, "$options": "i"}},
                        {"_base_name": {"$regex": sigla, "$options": "i"}},
                    ]
                }
                
                entradas_galpao = await db[COLLECTION_SLA_PEDIDOS_GALPAO].find(query_base).to_list(length=None)
                
                logger.debug(f"[SLA] Buscando em pedidos_no_galpao para base '{base_name}'. Encontradas {len(entradas_galpao)} entradas.")
                
                for entrada in entradas_galpao:
                    # Tentar múltiplos campos para número de pedido
                    numero_galpao = (
                        entrada.get("Número de pedido JMS", "") or
                        entrada.get("Remessa", "") or
                        entrada.get("Nº DO PEDIDO", "") or
                        entrada.get("NUMERO_PEDIDO", "")
                    )
                    if numero_galpao:
                        if isinstance(numero_galpao, str):
                            num_clean = numero_galpao.strip()
                            pedidos_no_galpao.add(num_clean)
                        else:
                            num_clean = str(numero_galpao).strip()
                            pedidos_no_galpao.add(num_clean)
                
                logger.debug(f"[SLA] Total de pedidos únicos no galpão: {len(pedidos_no_galpao)}")
                if pedidos_no_galpao:
                    logger.debug(f"[SLA] Exemplos de pedidos no galpão: {list(pedidos_no_galpao)[:5]}")
            except Exception as e:
                logger.error(f"Erro ao buscar pedidos no galpão: {e}")
                import traceback
                traceback.print_exc()
            
            # Dados básicos simples
            total_pedidos = len(records)
            entregues = sum(1 for record in records if record.get("Marca de assinatura", "").upper() == "RECEBIMENTO COM ASSINATURA NORMAL")
            
            # Contar "não entregues" EXCLUINDO pedidos no galpão
            nao_entregues = 0
            excluidos_galpao = 0
            for record in records:
                marca = record.get("Marca de assinatura", "").upper()
                numero_pedido = (
                    record.get("Número de pedido JMS", "") or 
                    record.get("Remessa", "") or 
                    record.get("Nº DO PEDIDO", "")
                )
                numero_pedido_str = str(numero_pedido).strip() if numero_pedido else ""
                
                # Só conta como "não entregue" se NÃO for entregue E NÃO estiver no galpão
                if marca != "RECEBIMENTO COM ASSINATURA NORMAL":
                    if numero_pedido_str and numero_pedido_str in pedidos_no_galpao:
                        excluidos_galpao += 1
                    elif numero_pedido_str:
                        nao_entregues += 1
            
            logger.debug(f"[SLA] Total não entregues (sem galpão): {nao_entregues}, Excluídos (no galpão): {excluidos_galpao}")
            
            # Agrupar por motorista (simples)
            motoristas = {}
            for record in records:
                motorista = record.get("Responsável pela entrega", "N/A")
                if motorista not in motoristas:
                    motoristas[motorista] = {"total": 0, "entregues": 0, "nao_entregues": 0}
                
                motoristas[motorista]["total"] += 1
                marca = record.get("Marca de assinatura", "").upper()
                numero_pedido = (
                    record.get("Número de pedido JMS", "") or 
                    record.get("Remessa", "") or 
                    record.get("Nº DO PEDIDO", "")
                )
                numero_pedido_str = str(numero_pedido).strip() if numero_pedido else ""
                esta_no_galpao = numero_pedido_str and numero_pedido_str in pedidos_no_galpao
                
                if marca == "RECEBIMENTO COM ASSINATURA NORMAL":
                    motoristas[motorista]["entregues"] += 1
                else:
                    # Só conta como "não entregue" se NÃO estiver no galpão
                    if esta_no_galpao:
                        # Não conta como não entregue (está no galpão)
                        pass
                    elif numero_pedido_str:
                        motoristas[motorista]["nao_entregues"] += 1
                
                # Debug específico para motorista "TAC ALEX DA SILVA"
                if "ALEX" in motorista.upper() and "SILVA" in motorista.upper():
                    if esta_no_galpao and marca != "RECEBIMENTO COM ASSINATURA NORMAL":
                        logger.debug(f"[SLA DEBUG] Motorista {motorista}: pedido {numero_pedido_str} está no galpão, não contado como não entregue")
                    elif marca != "RECEBIMENTO COM ASSINATURA NORMAL" and numero_pedido_str:
                        logger.debug(f"[SLA DEBUG] Motorista {motorista}: pedido {numero_pedido_str} contado como não entregue")
            
            # Buscar pedidos no galpão para cada motorista
            pedidos_galpao_por_motorista = {}
            try:
                # Buscar usando múltiplos formatos de base (mesmo padrão da verificação)
                import re
                sigla_match = re.search(r'([A-Z]{2,4})', base_name.upper())
                sigla = sigla_match.group(1) if sigla_match else ""
                
                query_base = {
                    "$or": [
                        {"_base_name": base_name},
                        {"_base_name": base_name.strip()},
                        {"Base de entrega": base_name},
                        {"Base de entrega": base_name.strip()},
                        {"Base de escaneamento": base_name},
                        {"Base de escaneamento": base_name.strip()},
                    ]
                }
                
                # Adicionar busca por sigla se encontrada
                if sigla:
                    query_base["$or"].extend([
                        {"_base_name": {"$regex": sigla, "$options": "i"}},
                        {"Base de entrega": {"$regex": sigla, "$options": "i"}},
                        {"Base de escaneamento": {"$regex": sigla, "$options": "i"}},
                    ])
                
                pedidos_galpao = await db[COLLECTION_SLA_PEDIDOS_GALPAO].find(query_base).to_list(length=None)
                
                logger.debug(f"[SLA] Buscando pedidos no galpão para base '{base_name}'. Encontrados: {len(pedidos_galpao)} pedidos.")
                
                for pedido_galpao in pedidos_galpao:
                    motorista_galpao = pedido_galpao.get("Responsável pela entrega", "N/A")
                    if motorista_galpao not in pedidos_galpao_por_motorista:
                        pedidos_galpao_por_motorista[motorista_galpao] = 0
                    pedidos_galpao_por_motorista[motorista_galpao] += 1
                
                if pedidos_galpao_por_motorista:
                    logger.debug(f"[SLA] Pedidos no galpão por motorista: {pedidos_galpao_por_motorista}")
            except Exception as e:
                logger.error(f"Erro ao buscar pedidos no galpão: {str(e)}")
                import traceback
                traceback.print_exc()
            
            # Preparar dados dos motoristas (simples)
            motoristas_data = []
            for motorista, dados in motoristas.items():
                # Buscar cidades deste motorista
                cidades_motorista = set()
                for record in records:
                    if record.get("Responsável pela entrega") == motorista:
                        cidade = record.get("Cidade Destino", "")
                        # Verificar se é string antes de chamar strip()
                        if cidade and isinstance(cidade, str) and cidade.strip():
                            cidades_motorista.add(cidade.strip())
                        elif cidade and not isinstance(cidade, str):
                            cidades_motorista.add(str(cidade))
                
                # Contar pedidos no galpão para este motorista
                pedidos_galpao_count = pedidos_galpao_por_motorista.get(motorista, 0)
                
                motoristas_data.append({
                    "motorista": motorista,
                    "total": dados["total"],
                    "entregues": dados["entregues"],
                    "naoEntregues": dados["nao_entregues"],
                    "pedidosGalpao": pedidos_galpao_count,
                    "percentual_entregues": round((dados["entregues"] / dados["total"] * 100), 2) if dados["total"] > 0 else 0,
                    "participacao": round((dados["total"] / total_pedidos * 100), 2) if total_pedidos > 0 else 0,
                    "todas_cidades": sorted(list(cidades_motorista))
                })
            
            # Ordenar por total
            motoristas_data.sort(key=lambda x: x["total"], reverse=True)
            
            return {
                "success": True,
                "base_name": base_name,
                "cities": cities,
                "motoristas": motoristas_data,
                "totais": {
                    "totalMotoristas": len(motoristas_data),
                    "totalPedidos": total_pedidos,
                    "entregues": entregues,
                    "naoEntregues": nao_entregues,
                    "taxaEntrega": round((entregues / total_pedidos * 100), 2) if total_pedidos > 0 else 0,
                    "slaMedio": round((entregues / total_pedidos * 100), 2) if total_pedidos > 0 else 0,
                    "motoristasExcelentes": len([m for m in motoristas_data if m.get("percentual_entregues", 0) >= 90])
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    async def get_available_cities(self, base_name: str) -> List[str]:
        """Retorna lista de cidades disponíveis"""
        try:
            db = self._get_database()
            
            # Buscar a base com múltiplos formatos (similar à busca do calculate_sla_metrics)
            import re
            sigla_match = re.search(r'([A-Z]{2,4})', base_name.upper())
            sigla = sigla_match.group(1) if sigla_match else ""
            
            # Tentar busca exata primeiro
            base_doc = await db[COLLECTION_SLA_BASES].find_one({"base_name": base_name})
            
            # Se não encontrar, tentar busca flexível
            if not base_doc:
                query = {
                    "$or": [
                        {"base_name": base_name.strip()},
                        {"base_name": {"$regex": re.escape(base_name.strip()), "$options": "i"}},
                    ]
                }
                
                # Adicionar busca por sigla se encontrada
                if sigla:
                    query["$or"].extend([
                        {"base_name": {"$regex": sigla, "$options": "i"}},
                    ])
                
                # Tentar encontrar qualquer base que contenha partes do nome
                base_doc = await db[COLLECTION_SLA_BASES].find_one(query)
                
                # Se ainda não encontrar, buscar todas e fazer matching manual
                if not base_doc:
                    all_bases = await db[COLLECTION_SLA_BASES].find({}).to_list(length=None)
                    base_normalized = base_name.strip().upper()
                    for base in all_bases:
                        base_db_name = base.get("base_name", "").upper().strip()
                        # Verificar se é exatamente igual ou contém a sigla
                        if base_normalized == base_db_name or (sigla and sigla in base_db_name):
                            base_doc = base
                            break
            
            cities = set()
            
            # 1. Buscar cidades de sla_bases (dados processados)
            if base_doc and "data" in base_doc:
                total_records = len(base_doc["data"])
                filtered_count = 0
                for record in base_doc["data"]:
                    # Filtrar pedidos movidos para o galpão
                    if record.get("status_galpao") == "movido_para_galpao":
                        filtered_count += 1
                        continue
                        
                    cidade = record.get("Cidade Destino", "")
                    # Verificar se é string antes de chamar strip()
                    if cidade and isinstance(cidade, str) and cidade.strip():
                        cities.add(cidade.strip())
                    elif cidade and not isinstance(cidade, str):
                        cities.add(str(cidade).strip())
                
                logger.info(f"Base '{base_name}': {total_records} registros em sla_bases, {filtered_count} filtrados por status_galpao, {len(cities)} cidades únicas encontradas")
            
            # 2. Buscar cidades de sla_chunks (dados não processados ainda)
            try:
                pipeline = [
                    {
                        "$match": {
                            "data": {
                                "$elemMatch": {
                                    "$or": [
                                        {"Base de entrega": base_name},
                                        {"Base de entrega": base_name.strip()},
                                        {"base": base_name},
                                        {"base": base_name.strip()},
                                        {"origem": base_name},
                                        {"origem": base_name.strip()},
                                    ]
                                }
                            }
                        }
                    },
                    {"$unwind": "$data"},
                    {
                        "$match": {
                            "$or": [
                                {"data.Base de entrega": base_name},
                                {"data.Base de entrega": base_name.strip()},
                                {"data.base": base_name},
                                {"data.base": base_name.strip()},
                                {"data.origem": base_name},
                                {"data.origem": base_name.strip()},
                            ]
                        }
                    },
                    {"$replaceRoot": {"newRoot": "$data"}},
                    {
                        "$group": {
                            "_id": "$Cidade Destino"
                        }
                    }
                ]
                
                # Adicionar busca por sigla se encontrada
                if sigla:
                    for match_stage in pipeline:
                        if "$match" in match_stage and "data" in match_stage["$match"]:
                            if "$elemMatch" in match_stage["$match"]["data"]:
                                match_stage["$match"]["data"]["$elemMatch"]["$or"].extend([
                                    {"Base de entrega": {"$regex": sigla, "$options": "i"}},
                                    {"base": {"$regex": sigla, "$options": "i"}},
                                ])
                
                cursor = db.sla_chunks.aggregate(pipeline)
                chunk_cities = await cursor.to_list(length=None)
                
                chunk_cities_count = 0
                for item in chunk_cities:
                    cidade = item.get("_id")
                    if cidade:
                        if isinstance(cidade, str) and cidade.strip():
                            cities.add(cidade.strip())
                            chunk_cities_count += 1
                        elif not isinstance(cidade, str):
                            cities.add(str(cidade).strip())
                            chunk_cities_count += 1
                
                if chunk_cities_count > 0:
                    logger.info(f"Base '{base_name}': {chunk_cities_count} cidades adicionais encontradas em sla_chunks")
            except Exception as e:
                logger.warning(f"Erro ao buscar cidades de sla_chunks para base '{base_name}': {e}")
            
            total_cities = len(cities)
            logger.info(f"Base '{base_name}': Total de {total_cities} cidades únicas encontradas")
            
            return sorted(list(cities))
            
        except Exception as e:
            logger.error(f"Erro ao buscar cidades para base '{base_name}': {e}")
            return []
    
    async def get_motorista_pedidos(self, base_name: str, motorista: str, status: Optional[str] = None, cidades: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Retorna pedidos de um motorista (simples)
        
        Args:
            base_name: Nome da base
            motorista: Nome do motorista
            status: Status opcional (entregues/nao_entregues)
            cidades: Lista de cidades opcional para filtrar
        """
        try:
            db = self._get_database()
            base_doc = await db[COLLECTION_SLA_BASES].find_one({"base_name": base_name})
            if not base_doc or "data" not in base_doc:
                return []
            
            # Normalizar lista de cidades para comparação
            cidades_norm = None
            if cidades:
                cidades_norm = [c.upper().strip() for c in cidades if c]
            
            pedidos = []
            for record in base_doc["data"]:
                # Filtrar pedidos movidos para o galpão
                if record.get("status_galpao") == "movido_para_galpao":
                    continue
                    
                if record.get("Responsável pela entrega") != motorista:
                    continue
                
                if status:
                    marca_assinatura = record.get("Marca de assinatura", "").upper()
                    if status.upper() == "ENTREGUES" and marca_assinatura != "RECEBIMENTO COM ASSINATURA NORMAL":
                        continue
                    elif status.upper() == "NAO_ENTREGUES" and marca_assinatura == "RECEBIMENTO COM ASSINATURA NORMAL":
                        continue
                
                # Filtrar por cidades (múltiplas ou nenhuma)
                if cidades_norm and len(cidades_norm) > 0:
                    cidade_destino = record.get("Cidade Destino", "")
                    # Normalizar cidade do registro
                    if isinstance(cidade_destino, str):
                        cidade_norm = cidade_destino.upper().strip()
                    else:
                        cidade_norm = str(cidade_destino).upper().strip()
                    
                    # Verificar se a cidade do pedido está na lista de cidades filtradas
                    if cidade_norm not in cidades_norm:
                        continue
                
                pedidos.append(record)
            
            return pedidos
            
        except Exception as e:
            logger.error(f"Erro em get_motorista_pedidos: {str(e)}")
            return []
