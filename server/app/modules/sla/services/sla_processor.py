import pandas as pd
import uuid
from typing import List, Dict, Any, Tuple
from datetime import datetime
from app.services.database import get_database
from app.modules.sla.models.sla_chunk import SLAChunk, SLAFile

class SLAProcessor:
    """Processador de arquivos SLA"""
    
    def __init__(self):
        self.db = None
        self.chunk_size = 1000  # Processar 1000 registros por chunk
    
    def _get_database(self):
        """Obtém a instância do banco de dados (lazy loading)"""
        if self.db is None:
            self.db = get_database()
            if self.db is None:
                raise Exception("Database não está conectado. Verifique a conexão com MongoDB.")
        return self.db
        
    async def process_file(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        """
        Processa arquivo SLA e salva em chunks
        
        Args:
            file_content: Conteúdo do arquivo em bytes
            filename: Nome do arquivo
            
        Returns:
            Dict com informações do processamento
        """
        try:
            # Gerar ID único para o arquivo
            file_id = str(uuid.uuid4())
            
            # Ler arquivo Excel
            try:
                df = pd.read_excel(file_content, engine='openpyxl')
            except Exception as e:
                # Tentar como CSV se Excel falhar
                try:
                    file_content_str = file_content.decode('utf-8')
                    df = pd.read_csv(file_content_str)
                except Exception as e2:
                    raise ValueError(f"Não foi possível ler o arquivo: {str(e)}")
            
            # Validar se há dados
            if len(df) == 0:
                raise ValueError("Arquivo não contém dados")
            
            
            # Extrair bases únicas
            unique_bases = self._extract_unique_bases(df)
            
            # Criar registro do arquivo
            sla_file = SLAFile(
                filename=filename,
                file_size=len(file_content),
                total_chunks=0,
                total_records=len(df),
                unique_bases=unique_bases,
                status="processing"
            )
            
            # Salvar arquivo no banco
            db = self._get_database()
            file_doc = await db.sla_files.insert_one(sla_file.dict())
            file_id = str(file_doc.inserted_id)
            
            # Processar dados em chunks
            chunks_created = await self._create_chunks(df, file_id)
            
            # Atualizar arquivo com total de chunks
            await db.sla_files.update_one(
                {"_id": file_doc.inserted_id},
                {
                    "$set": {
                        "total_chunks": chunks_created,
                        "status": "completed"
                    }
                }
            )
            
            
            return {
                "success": True,
                "file_id": file_id,
                "total_records": len(df),
                "total_chunks": chunks_created,
                "message": f"Arquivo processado com sucesso. {chunks_created} chunks criados."
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"Erro ao processar arquivo: {str(e)}"
            }
    
    async def _create_chunks(self, df: pd.DataFrame, file_id: str) -> int:
        """
        Cria chunks dos dados e salva no banco
        
        Args:
            df: DataFrame com os dados
            file_id: ID do arquivo
            
        Returns:
            Número de chunks criados
        """
        total_records = len(df)
        total_chunks = (total_records + self.chunk_size - 1) // self.chunk_size
        
        chunks_created = 0
        
        for i in range(0, total_records, self.chunk_size):
            chunk_data = df.iloc[i:i + self.chunk_size]
            
            # Converter DataFrame para lista de dicionários
            chunk_records = chunk_data.to_dict('records')
            
            # Criar chunk
            chunk = SLAChunk(
                chunk_index=chunks_created,
                total_chunks=total_chunks,
                file_id=file_id,
                data=chunk_records,
                status="completed"
            )
            
            # Salvar chunk no banco
            db = self._get_database()
            await db.sla_chunks.insert_one(chunk.dict())
            chunks_created += 1
            
        
        return chunks_created
    
    async def get_file_stats(self, file_id: str) -> Dict[str, Any]:
        """
        Obtém estatísticas de um arquivo processado
        
        Args:
            file_id: ID do arquivo
            
        Returns:
            Dict com estatísticas
        """
        try:
            # Buscar arquivo
            db = self._get_database()
            file_doc = await db.sla_files.find_one({"_id": file_id})
            if not file_doc:
                return {"error": "Arquivo não encontrado"}
            
            # Contar chunks
            chunks_count = await db.sla_chunks.count_documents({"file_id": file_id})
            
            # Contar registros processados
            pipeline = [
                {"$match": {"file_id": file_id}},
                {"$project": {"data_count": {"$size": "$data"}}},
                {"$group": {"_id": None, "total": {"$sum": "$data_count"}}}
            ]
            
            result = await db.sla_chunks.aggregate(pipeline).to_list(1)
            total_records = result[0]["total"] if result else 0
            
            return {
                "file_id": file_id,
                "filename": file_doc["filename"],
                "total_chunks": chunks_count,
                "total_records": total_records,
                "status": file_doc["status"],
                "created_at": file_doc["created_at"],
                "processed_at": file_doc.get("processed_at")
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    async def get_chunk_data(self, file_id: str, chunk_index: int) -> Dict[str, Any]:
        """
        Obtém dados de um chunk específico
        
        Args:
            file_id: ID do arquivo
            chunk_index: Índice do chunk
            
        Returns:
            Dict com dados do chunk
        """
        try:
            db = self._get_database()
            chunk_doc = await db.sla_chunks.find_one({
                "file_id": file_id,
                "chunk_index": chunk_index
            })
            
            if not chunk_doc:
                return {"error": "Chunk não encontrado"}
            
            return {
                "chunk_index": chunk_doc["chunk_index"],
                "total_chunks": chunk_doc["total_chunks"],
                "data": chunk_doc["data"],
                "status": chunk_doc["status"],
                "created_at": chunk_doc["created_at"]
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    async def get_global_stats(self) -> Dict[str, Any]:
        """
        Obtém estatísticas globais do sistema SLA
        
        Returns:
            Dict com estatísticas globais
        """
        try:
            # Contar arquivos
            db = self._get_database()
            total_files = await db.sla_files.count_documents({})
            
            # Contar chunks
            total_chunks = await db.sla_chunks.count_documents({})
            
            # Contar registros totais
            pipeline = [
                {"$project": {"data_count": {"$size": "$data"}}},
                {"$group": {"_id": None, "total": {"$sum": "$data_count"}}}
            ]
            
            result = await db.sla_chunks.aggregate(pipeline).to_list(1)
            total_records = result[0]["total"] if result else 0
            
            # Último processamento (usar created_at já que processed_at não existe)
            last_processed = await db.sla_files.find_one(
                {"status": "completed"},
                sort=[("created_at", -1)]
            )
            
            # Converter datetime para string ISO se existir
            last_processed_date = None
            if last_processed and last_processed.get("created_at"):
                created_at = last_processed["created_at"]
                if isinstance(created_at, datetime):
                    last_processed_date = created_at.isoformat()
                else:
                    last_processed_date = str(created_at)
            
            return {
                "total_files": total_files,
                "total_chunks": total_chunks,
                "total_records": total_records,
                "last_processed": last_processed_date
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    def _extract_unique_bases(self, df: pd.DataFrame) -> List[str]:
        """
        Extrai bases únicas do DataFrame, priorizando a coluna "Base de entrega"
        
        Args:
            df: DataFrame com os dados
            
        Returns:
            Lista de bases únicas
        """
        try:
            unique_bases = set()
            
            # Primeiro, procurar especificamente pela coluna "Base de entrega"
            if "Base de entrega" in df.columns:
                values = df["Base de entrega"].dropna().unique()
                for value in values:
                    if pd.notna(value) and str(value).strip():
                        unique_bases.add(str(value).strip())
            
            # Se não encontrou na coluna específica, procurar por outras colunas relacionadas
            if not unique_bases:
                base_columns = ['base', 'Base', 'BASE', 'base_origem', 'origem', 'filial', 'filial_origem', 'base_entrega']
                
                for col in df.columns:
                    if any(base_name in col.lower() for base_name in ['base', 'origem', 'filial']):
                        values = df[col].dropna().unique()
                        for value in values:
                            if pd.notna(value) and str(value).strip():
                                unique_bases.add(str(value).strip())
            
            # Se ainda não encontrou, procurar em todas as colunas por padrões de base
            if not unique_bases:
                for col in df.columns:
                    sample_values = df[col].dropna().head(10)
                    for value in sample_values:
                        if pd.notna(value):
                            str_value = str(value).strip()
                            # Verificar se parece ser uma base (contém letras, números e hífen)
                            if (len(str_value) <= 20 and 
                                any(c.isalpha() for c in str_value) and 
                                ('-' in str_value or ' ' in str_value)):
                                unique_bases.add(str_value)
            
            # Converter para lista ordenada
            result = sorted(list(unique_bases))
            return result
            
        except Exception as e:
            return []
    
    async def get_all_unique_bases(self) -> Dict[str, Any]:
        """
        Obtém todas as bases únicas de todos os arquivos SLA processados
        
        Returns:
            Dict com lista de bases únicas
        """
        try:
            db = self._get_database()
            
            # Buscar todos os arquivos que têm bases
            files_with_bases = await db.sla_files.find(
                {"unique_bases": {"$exists": True, "$ne": []}},
                {"unique_bases": 1, "filename": 1}
            ).to_list(None)
            
            # Coletar todas as bases únicas
            all_bases = set()
            for file_doc in files_with_bases:
                if "unique_bases" in file_doc:
                    for base in file_doc["unique_bases"]:
                        if base and base.strip():
                            all_bases.add(base.strip())
            
            # Converter para lista ordenada
            unique_bases = sorted(list(all_bases))
            
            
            return {
                "bases": unique_bases,
                "total_files": len(files_with_bases),
                "total_bases": len(unique_bases)
            }
            
        except Exception as e:
            return {"error": str(e)}
