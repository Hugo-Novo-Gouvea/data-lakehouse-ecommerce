# 🛍️ Data Lakehouse E-commerce

Pipeline de Engenharia de Dados End-to-End simulando um ambiente de Lakehouse moderno, utilizando arquitetura Medalhão (Bronze, Silver, Gold).

## 🏗️ Arquitetura e Tecnologias

Este projeto utiliza uma stack moderna baseada em containers:

* **Infraestrutura:** Docker & Docker Compose.
* **Orquestração:** Apache Airflow (DAGs agendadas).
* **Data Lake:** MinIO (S3 Compatible Object Storage).
* **Ingestão & Transformação:** Python, Pandas e Boto3.
* **Analytics (SQL on Files):** DuckDB (Processamento OLAP em arquivos Parquet).

## 🔄 Fluxo de Dados (ETL)

1.  **Camada Bronze (Raw):** Ingestão de dados brutos da [FakeStore API](https://fakestoreapi.com/) em formato **JSON**. Garante a fidelidade do dado original.
2.  **Camada Silver (Refined):** Limpeza, desnormalização de campos aninhados (JSON Flattening) e conversão para formato colunar **Parquet** com compressão Snappy.
3.  **Camada Gold (Aggregated):** Execução de queries SQL analíticas via DuckDB para gerar KPIs de negócios (Média de preços e avaliações por categoria), exportados para **CSV**.

## 🚀 Como Executar

### Pré-requisitos
* Docker e Docker Compose instalados.

### Passo a Passo
1.  Clone o repositório:
    ```bash
    git clone [https://github.com/SEU_USUARIO/data-lakehouse-ecommerce.git](https://github.com/SEU_USUARIO/data-lakehouse-ecommerce.git)
    ```
2.  Configure as variáveis de ambiente criando um .env:
    ```bash
    .env
    # Ajuste as credenciais se necessário
    ```
3.  Suba o ambiente:
    ```bash
    docker compose up -d
    ```
4.  Acesse o Airflow (`http://localhost:8080`) e ative a DAG `pipeline_ecommerce_completo`.
5.  Acesse o MinIO (`http://localhost:9001`) para visualizar os buckets e arquivos gerados.

---
*Desenvolvido como projeto prático de Engenharia de Dados.*
