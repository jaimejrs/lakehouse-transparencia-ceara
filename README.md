# Lakehouse de transparência pública do Ceará

Pipeline de dados para integrar contratos, empenhos e ordens bancárias do Ceará em uma base analítica consultável. A solução ingere dados de uma API pública e de PostgreSQL, preserva o histórico bruto, aplica deduplicação incremental, constrói um modelo dimensional e disponibiliza análises financeiras, detecção de anomalias e previsão de pagamentos.

![CI](https://github.com/jaimejrs/data-engineering-lab-PY03/actions/workflows/ci.yml/badge.svg)

![Arquitetura do lakehouse](imgs/arquitetura-diagrama.svg)

## O que o projeto entrega

- **Ingestão incremental:** contratos da API Ceará Transparente e tabelas de empenhos, ordens bancárias e unidades gestoras de um PostgreSQL de origem, com paginação, processamento em lotes, watermark e validação.
- **Lakehouse Bronze → Silver → Gold:** JSON bruto em HDFS; tabelas Apache Iceberg em HDFS com Hive Metastore; transformação Silver em PySpark com `MERGE INTO` para evitar duplicatas entre cargas; marts dimensionais em dbt e Trino.
- **Consumo analítico:** fatos de contratos, empenhos e ordens bancárias, dimensões de órgão, credor, modalidade e tempo; consultas via Trino; painel Streamlit e monitoramento operacional em Superset.
- **Modelos e relatórios:** Isolation Forest para priorizar contratos atípicos, XGBoost quantílico para prever pagamentos trimestrais e relatório narrativo gerado a partir dos resultados calculados.
- **Operação e qualidade:** quatro DAGs Airflow encadeadas por Dataset, testes dbt, reconciliações, testes Python e integração contínua no GitHub Actions.

## Arquitetura

| Camada | Implementação | Responsabilidade |
|---|---|---|
| Fontes | API REST + PostgreSQL externo | Contratos, empenhos, ordens bancárias e unidades gestoras |
| Bronze | Python + WebHDFS, JSON | Preservar os registros extraídos e validar a entrada |
| Silver | PySpark + Apache Iceberg | Normalizar, tipar e deduplicar por chave de negócio |
| Gold | dbt + Trino + Apache Iceberg | Modelo estrela, histórico de credores com SCD2 e testes |
| ML e auditoria | Iceberg em schemas `ml` e `audit` | Scores, previsões, relatórios, reconciliação e métricas operacionais |
| Consumo | Trino, Streamlit, Superset | SQL, painéis de negócio e observabilidade |

O Airflow executa `bronze_extract → silver_transform → gold_load → ml_inference`. Cada etapa publica um Dataset consumido pela seguinte. O [diagrama técnico](documentacao/diagrama-arquitetura.md) detalha serviços, fluxos e decisões de arquitetura.

## Evidências e limites

Em uma validação registrada em **28/07/2026**, o ambiente do projeto continha, na Silver, **1.376.379 empenhos**, **1.399.810 ordens bancárias**, **216.358 contratos** e **5.011 unidades gestoras**. Esses números são um retrato daquela execução, não uma contagem atual em tempo real. O catálogo de qualidade descreve **62 testes dbt** entre modelos, fontes e regras de negócio; a integração contínua executa também testes Python, Spark e uma integração dbt em ambiente efêmero. Consulte [testes dbt](documentacao/testes-dbt.md) e a [documentação da Gold](documentacao/gold-dbt-trino.md).

A junção direta entre contratos da API e empenhos do PostgreSQL **não é garantida por uma chave única comum**. O projeto usa dimensões confiáveis para análise e trata vínculos entre processos/contratos como enriquecimento sujeito à cobertura documentada no [dicionário de dados](documentacao/dicionario-dados.md). Os scores de anomalia são sinais para investigação, não conclusões de irregularidade.

## Executar localmente

**Pré-requisitos:** Docker Compose, acesso ao PostgreSQL de origem e credenciais adequadas para a fonte. A API de contratos é pública. O relatório narrativo requer `OPENAI_API_KEY`; as outras etapas devem ser configuradas conforme o ambiente. O stack completo usa vários serviços e exige recursos de máquina compatíveis.

```bash
git clone https://github.com/jaimejrs/data-engineering-lab-PY03.git
cd data-engineering-lab-PY03
cp .env.example .env
# Configure SOURCE_POSTGRES_URL, caminhos de armazenamento e demais valores em .env
docker compose up -d --build
```

Abra o Airflow em `http://localhost:8080`, habilite as DAGs `bronze_extract`, `silver_transform`, `gold_load` e `ml_inference`, e inicie a primeira:

```bash
docker exec datalab_airflow_scheduler airflow dags trigger bronze_extract
```

A interface do HDFS fica em `http://localhost:9870`; o Trino é publicado em `localhost:8085`. O PostgreSQL de **origem** não é provisionado pelo Compose: a variável `SOURCE_POSTGRES_URL` deve apontar para uma fonte acessível. O painel Streamlit e o Superset do ambiente compartilhado estão no [overlay de implantação](deploy/server-lakehouse/README.md), e não sobem automaticamente pelo Compose da raiz. A [configuração de ambiente](.env.example) e o [guia de exploração](documentacao/guia-de-exploracao.md) trazem as demais opções e consultas de exemplo.

Para desenvolver ou validar sem executar o pipeline completo:

```bash
python -m pytest tests/ -v
ruff check .
ruff format --check .
```

Os testes unitários usam mocks para serviços externos; os jobs de integração e o pipeline completo exigem suas respectivas dependências. A configuração de CI está em [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

## Código e documentação

| Diretório | Conteúdo |
|---|---|
| [`dags/`](dags/) | Orquestração Airflow e gatilhos entre camadas |
| [`src/extractors/`](src/extractors/) | Extração da API/PostgreSQL e escrita na Bronze |
| [`src/spark_jobs/`](src/spark_jobs/) | Transformação e carga incremental da Silver |
| [`dbt/`](dbt/) | Staging, dimensões, fatos, snapshot SCD2 e testes |
| [`models/`](models/) | Detecção de anomalias, previsão e relatório |
| [`streamlit/`](streamlit/) | Painel analítico de negócio |
| [`deploy/server-lakehouse/`](deploy/server-lakehouse/) | Serviços e rotinas do ambiente compartilhado |
| [`documentacao/`](documentacao/) | Arquitetura, dicionário, decisões e guias |
| [`tests/`](tests/) | Testes de extração, transformação e modelos |

As [capturas de tela](imgs/) mostram Airflow, HDFS, Trino, MLflow e painéis. A documentação técnica registra as escolhas, limitações e procedimentos de operação.
