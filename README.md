## AIRFLOW - On premisses SIAN

### 1. Introdução
#### 1.1 Propósito do Documento
* Este documento visa apresentar a estrutura, implementação e potenciais benefícios da solução de dados implementada para a organização SIAN e suas empresas clientes (HP Transportes, ITA e URBI). O foco está em demonstrar o potencial de transformação e otimização do negócio por meio da coleta, processamento e análise de dados em um ambiente de dados integrado.

#### 1.2 Escopo da Implementação
* A implementação abrange a coleta de dados de diversas fontes, processamento desses dados e a criação de dashboards interativos para análise e tomada de decisão. O escopo inclui:
Coleta de dados de múltiplas fontes.
* Armazenamento de dados em buckets do Google Cloud Platform (GCP).
* Processamento e análise de dados com BigQuery.
* Desenvolvimento de dashboards para visualização de insights.
* Criação de documentação detalhada.
### 2. Objetivos da Implementação
* Demonstrar o impacto da integração de dados no aumento da eficiência operacional.
* Monitorar e analisar dados provenientes de diversas fontes para oferecer insights que melhorem a tomada de decisão e otimizem processos de negócios.
### 3. Tecnologias Utilizadas
* Plataforma de Armazenamento de Dados: Google Cloud Storage (GCS).
* Banco de Dados: BigQuery.
* Ferramentas de ETL: Apache Airflow e DBT.
* Plataforma de Dashboard: Power BI.
### 4. Arquitetura da Solução
* A arquitetura da solução consiste em uma combinação das ferramentas de ETL e de armazenamento e processamento de dados do Google Cloud Platform. A estrutura é organizada pela SIAN, e os dados são segregados por empresa cliente (HP, ITA, URBI), garantindo que apenas a SIAN tenha acesso a todos os dados, enquanto as empresas têm acesso apenas aos seus próprios dados.



### 5. Implementação
#### 5.1 Coleta de Dados
* A coleta de dados consiste em criar pipelines no Apache Airflow para obter dados de diversas fontes, armazenando-os nos buckets do GCP.
#### 5.2 Processamento de Dados
* O processamento dos dados é realizado utilizando DBT e BigQuery, onde os dados são transformados e documentados.
#### 5.3 Desenvolvimento dos Dashboards/Relatórios
* Disponibilização de dados para a área de operações para confecção de dashboards.
* Envio de relatórios por email (caso solicitado).
