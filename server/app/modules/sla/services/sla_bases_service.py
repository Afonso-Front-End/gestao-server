from typing import List, Dict, Any, Optional
from datetime import datetime
from app.services.database import get_database
from app.modules.sla.models.sla_bases_data import SLABaseData, SLABaseStats

class SLABasesService:
    """Serviço para processar dados de bases SLA"""
    
    def __init__(self):
        self.db = None
    
    def _get_database(self):
        """Obtém a instância do banco de dados (lazy loading)"""
        if self.db is None:
            self.db = get_database()
            if self.db is None:
                raise Exception("Database não está conectado. Verifique a conexão com MongoDB.")
        return self.db
    
    async def process_selected_bases(self, selected_bases: List[str]) -> Dict[str, Any]:
        """
        Processa as bases selecionadas e salva os dados
        
        Args:
            selected_bases: Lista de bases selecionadas
            
        Returns:
            Dict com resultado do processamento
        """
        try:
            
            results = []
            total_processed = 0
            
            for base_name in selected_bases:
                
                # Verificar se a base já existe
                db = self._get_database()
                existing_base = await db.sla_bases_data.find_one({"base_name": base_name})
                
                if existing_base:
                    continue
                
                # Buscar dados da base nos chunks SLA
                base_data = await self._get_base_data(base_name)
                
                if base_data:
                    # Criar documento da base
                    sla_base = SLABaseData(
                        base_name=base_name,
                        total_records=len(base_data),
                        total_pedidos=len(set(record.get('Número de pedido JMS', '') for record in base_data if record.get('Número de pedido JMS', ''))),
                        data=base_data,
                        status="processed"
                    )
                    
                    # Salvar no banco
                    result = await db.sla_bases_data.insert_one(sla_base.dict())
                    
                    
                    results.append({
                        "base_name": base_name,
                        "total_records": len(base_data),
                        "total_pedidos": len(set(record.get('Número de pedido JMS', '') for record in base_data if record.get('Número de pedido JMS', '')))
                    })
                    
                    total_processed += 1
                else:
                    continue
            
            return {
                "success": True,
                "total_bases_processed": total_processed,
                "results": results,
                "message": f"Processamento concluído: {total_processed} bases processadas"
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": "Erro no processamento das bases"
            }
    
    async def _get_base_data(self, base_name: str) -> List[Dict[str, Any]]:
        """
        Busca todos os dados de uma base específica nos chunks SLA
        
        Args:
            base_name: Nome da base
            
        Returns:
            Lista com todos os dados da base
        """
        try:
            db = self._get_database()
            
            # Buscar em todos os chunks que contêm dados desta base
            pipeline = [
                {
                    "$match": {
                        "data": {
                            "$elemMatch": {
                                "$or": [
                                    {"Base de entrega": base_name},
                                    {"base": base_name},
                                    {"origem": base_name}
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
                            {"data.base": base_name},
                            {"data.origem": base_name}
                        ]
                    }
                },
                {"$replaceRoot": {"newRoot": "$data"}}
            ]
            
            cursor = db.sla_chunks.aggregate(pipeline)
            base_data = await cursor.to_list(None)
            
            # Deduplicar por número JMS e filtrar apenas pedidos pai
            numeros_jms_unicos = set()
            dados_unicos = []
            
            for record in base_data:
                numero_jms = record.get("Número de pedido JMS", "")
                
                # Pular se já processamos este número JMS
                if numero_jms and numero_jms in numeros_jms_unicos:
                    continue
                
                # Verificar se é pedido pai (não tem pedidos filhos)
                # Pedidos filhos geralmente têm caracteres especiais ou sufixos
                if numero_jms:
                    # Verificar se é pedido pai baseado no padrão do número
                    is_pedido_pai = self._is_pedido_pai(numero_jms, base_data)
                    
                    if is_pedido_pai:
                        numeros_jms_unicos.add(numero_jms)
                        dados_unicos.append(record)
            
            return dados_unicos
            
        except Exception as e:
            return []
    
    def _is_pedido_pai(self, numero_jms: str, all_records: list) -> bool:
        """
        Verifica se um número JMS é um pedido pai (não tem filhos)
        
        Args:
            numero_jms: Número do pedido JMS
            all_records: Lista com todos os registros
            
        Returns:
            True se for pedido pai, False se for pedido filho
        """
        try:
            # Estratégia simples: Pedidos pai não têm hífen, pedidos filhos têm
            # Exemplo: 888001229814813 (pai) vs 888001229814813-001 (filho)
            
            if '-' in numero_jms:
                # Se tem hífen, é pedido filho
                return False
            else:
                # Se não tem hífen, é pedido pai
                return True
            
        except Exception as e:
            return True  # Em caso de erro, considerar como pedido pai
    
    
    async def get_base_stats(self, base_name: str) -> Dict[str, Any]:
        """
        Obtém estatísticas de uma base específica
        
        Args:
            base_name: Nome da base
            
        Returns:
            Dict com estatísticas da base
        """
        try:
            db = self._get_database()
            
            base_doc = await db.sla_bases_data.find_one({"base_name": base_name})
            
            if not base_doc:
                return {"error": "Base não encontrada"}
            
            return {
                "base_name": base_doc["base_name"],
                "total_records": base_doc["total_records"],
                "total_pedidos": base_doc["total_pedidos"],
                "last_processed": base_doc["updated_at"].isoformat() if base_doc.get("updated_at") else None,
                "status": base_doc["status"]
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    async def get_all_bases_stats(self) -> Dict[str, Any]:
        """
        Obtém estatísticas de todas as bases processadas
        
        Returns:
            Dict com estatísticas globais
        """
        try:
            db = self._get_database()
            
            # Contar total de bases
            total_bases = await db.sla_bases_data.count_documents({})
            
            # Agregar estatísticas
            pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "total_records": {"$sum": "$total_records"},
                        "total_pedidos": {"$sum": "$total_pedidos"}
                    }
                }
            ]
            
            result = await db.sla_bases_data.aggregate(pipeline).to_list(1)
            stats = result[0] if result else {
                "total_records": 0,
                "total_pedidos": 0
            }
            
            # Última atualização
            last_updated = await db.sla_bases_data.find_one(
                {},
                sort=[("updated_at", -1)]
            )
            
            return {
                "total_bases": total_bases,
                "total_records": stats["total_records"],
                "total_pedidos": stats["total_pedidos"],
                "last_updated": last_updated["updated_at"].isoformat() if last_updated and last_updated.get("updated_at") else None
            }
            
        except Exception as e:
            return {"error": str(e)}
