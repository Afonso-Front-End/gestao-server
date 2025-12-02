# Torre de Controle - Backend

Sistema de controle para gerenciamento de pedidos retidos, lista de telefones e SLA.

## 🏗️ Estrutura do Projeto

```
server/
├── app/
│   ├── core/                      # Configurações centrais
│   │   ├── __init__.py
│   │   ├── collections.py         # Definições de coleções MongoDB
│   │   └── exceptions.py           # Exceções customizadas
│   ├── modules/                   # Módulos da aplicação
│   │   ├── retidos/               # Módulo de pedidos retidos
│   │   │   ├── models/
│   │   │   │   └── pedidos_retidos.py
│   │   │   ├── routes/            # Endpoints de pedidos retidos
│   │   │   │   ├── upload.py
│   │   │   │   ├── bases.py
│   │   │   │   ├── filtros.py
│   │   │   │   ├── selects.py
│   │   │   │   ├── relatorio.py
│   │   │   │   ├── motorista.py
│   │   │   │   └── helpers.py
│   │   │   └── services/
│   │   │       └── excel_processor.py
│   │   ├── telefones/             # Módulo de lista de telefones
│   │   │   ├── models/
│   │   │   │   └── lista_telefones.py
│   │   │   ├── routes/            # Endpoints de telefones
│   │   │   │   ├── upload.py
│   │   │   │   ├── listar.py
│   │   │   │   ├── chunks.py
│   │   │   │   └── motorista.py
│   │   │   └── services/
│   │   │       └── lista_telefones_service.py
│   │   └── sla/                   # Módulo de SLA
│   │       ├── models/
│   │       │   ├── sla_bases_data.py
│   │       │   ├── sla_chunk.py
│   │       │   └── galpao_entradas.py
│   │       ├── routes/            # Endpoints de SLA
│   │       │   ├── upload.py
│   │       │   ├── stats.py
│   │       │   ├── bases.py
│   │       │   ├── health.py
│   │       │   ├── galpao_upload.py
│   │       │   ├── sla_bases/
│   │       │   ├── sla_calculator/
│   │       │   └── pedidos_galpao/
│   │       └── services/
│   │           ├── sla_bases_service.py
│   │           ├── sla_calculator.py
│   │           ├── sla_processor.py
│   │           └── galpao_service.py
│   ├── routes/                    # Rotas administrativas
│   │   └── admin.py
│   ├── services/                  # Serviços compartilhados
│   │   ├── database.py            # Conexão com MongoDB
│   │   └── excel_processor.py    # Processamento de Excel
│   └── main.py                    # Aplicação principal FastAPI
├── requirements.txt               # Dependências
└── README.md                      # Este arquivo
```

## 🚀 Como Executar

### 1. Instalar Dependências
```bash
pip install -r requirements.txt
```

### 2. Configurar Variáveis de Ambiente

**Opção 1: Usando arquivo `.env` (Recomendado)**

Copie o arquivo `.env.example` para `.env` e configure:

```bash
cp .env.example .env
# Edite o arquivo .env com suas configurações
```

**Opção 2: Exportar no terminal**

```bash
# MongoDB
export MONGODB_URL=mongodb://localhost:27017
export DATABASE_NAME=bdlogistica

# Servidor (IMPORTANTE: mude PORT se houver conflito)
export PORT=8001  # Altere se necessário (ex: 8080, 3001, etc)
export HOST=0.0.0.0
export DEBUG=false
```

**⚠️ IMPORTANTE - Configuração de Porta:**

Se você mudar a `PORT` do servidor, também precisa atualizar:

1. **Frontend (Vite)**: Configure `VITE_API_PORT` no arquivo `app/.env`
2. **Tauri**: Configure via localStorage:
   ```javascript
   localStorage.setItem('api_port', '8080')  // Sua nova porta
   // OU
   localStorage.setItem('api_base_url', 'http://localhost:8080/api')
   ```

### 3. Executar o Servidor

Opção 1: Usando Python diretamente
```bash
cd server
python -m app.main
```

Opção 2: Usando uvicorn diretamente
```bash
cd server
uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

## 📚 Documentação da API

Após iniciar o servidor, acesse:
- **Swagger UI**: http://localhost:8001/docs
- **ReDoc**: http://localhost:8001/redoc

## 🔧 Configurações

### Variáveis de Ambiente Principais

- `MONGODB_URL`: URL de conexão com MongoDB
- `DATABASE_NAME`: Nome do banco de dados
- `DEBUG`: Modo debug (true/false)
- `LOG_LEVEL`: Nível de logging (DEBUG, INFO, WARNING, ERROR)
- `MAX_FILE_SIZE`: Tamanho máximo de arquivo (bytes)

### CORS

As origens permitidas são configuráveis via `ALLOWED_ORIGINS` no arquivo de configuração.

## 🏛️ Arquitetura

### Camadas da Aplicação

1. **API Layer** (`/modules/*/routes/`): Endpoints REST organizados por módulo
2. **Service Layer** (`/modules/*/services/` e `/services/`): Lógica de negócio
3. **Model Layer** (`/modules/*/models/`): Modelos de dados Pydantic
4. **Core Layer** (`/core/`): Configurações e utilitários compartilhados

### Padrões Utilizados

- **Modular Architecture**: Organização por módulos (retidos, telefones, sla)
- **Service Layer Pattern**: Para lógica de negócio
- **Dependency Injection**: Para injeção de dependências
- **Repository Pattern**: Para acesso a dados via MongoDB/Motor

## 🔍 Logging

O sistema possui logging centralizado com diferentes níveis:
- **INFO**: Operações normais
- **WARNING**: Situações de atenção
- **ERROR**: Erros que não impedem execução
- **CRITICAL**: Erros críticos

## 🛡️ Segurança

- Validação de tamanho de arquivo
- Headers de segurança
- Tratamento de erros padronizado
- CORS configurável

## 📊 Endpoints Principais

### Pedidos Retidos
- Rotas organizadas em `/modules/retidos/routes/`
- Upload, filtros, bases, relatórios, estatísticas

### Lista Telefones
- Rotas organizadas em `/modules/telefones/routes/`
- Upload, listagem, chunks, motorista

### SLA
- Rotas organizadas em `/modules/sla/routes/`
- Upload de bases, cálculo de SLA, estatísticas, galpão

Para ver todos os endpoints disponíveis, acesse a documentação interativa em `/docs`

## 🔧 Desenvolvimento

### Estrutura de Commits
- `feat:` Nova funcionalidade
- `fix:` Correção de bug
- `refactor:` Refatoração
- `docs:` Documentação
- `test:` Testes

### Code Style
- Black para formatação
- isort para organização de imports
- flake8 para linting