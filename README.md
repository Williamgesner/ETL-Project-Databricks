# 📊 ETL Pipeline — Databricks + Google Drive + Power BI

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?&logo=python&logoColor=white)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-150458?&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Google Sheets](https://img.shields.io/badge/Google%20Sheets-34A853?&logo=googlesheets&logoColor=white)](https://www.google.com/sheets/about/)
[![Google Drive](https://img.shields.io/badge/Google%20Drive-4285F4?&logo=googledrive&logoColor=white)](https://drive.google.com/)
[![Google Cloud](https://img.shields.io/badge/Google%20Cloud-Service%20Account-4285F4?&logo=googlecloud&logoColor=white)](https://cloud.google.com/)
[![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Power Automate](https://img.shields.io/badge/Power%20Automate-0066FF?&logo=powerautomate&logoColor=white)](https://powerautomate.microsoft.com/)
[![Git](https://img.shields.io/badge/Git-F05032?&logo=git&logoColor=white)](https://git-scm.com/)

Pipeline de dados desenvolvido inteiramente no **Databricks**, com carga incremental automática e dashboard atualizado em tempo real.

---

## 🧩 Visão Geral

```
Google Sheets (RAW) → Databricks (ETL) → Google Drive (Processed) → Power BI (Dashboard)
```

O cliente preenche manualmente uma planilha no Google Sheets (schema RAW). O pipeline extrai esses dados, aplica as transformações e modelos necessários, e salva o resultado em uma planilha separada no Google Drive (schema Processed). Essa planilha processada é a fonte de dados do Power BI.

---

## ⚙️ Stack Utilizada

| Camada | Tecnologia |
|---|---|
| Orquestração | Databricks Jobs (Serverless) |
| Processamento | Python + Pandas |
| Fonte de dados | Google Sheets (API v4) |
| Armazenamento | Google Drive |
| Autenticação | Google Service Account |
| Visualização | Power BI |
| Automação de refresh | Power Automate |

---

## 🗂️ Estrutura do Projeto

```
├── config/
│   └── settings.py              # Credenciais (Service Account) e IDs dos arquivos
├── extract/
│   └── gdrive_connector.py      # Conexão e extração das abas do Google Sheets
├── transform/
│   ├── categorias.py
│   ├── contatos.py
│   ├── contas_pagar.py
│   ├── vendas_servicos.py
│   ├── caixa.py
│   └── tempo.py                 # Script standalone para popular dim_tempo
├── models/
│   ├── dim_categorias.py
│   ├── dim_contatos.py
│   ├── dim_tempo.py
│   ├── fato_contas_pagar.py
│   ├── fato_vendas_servicos.py
│   └── fato_caixa.py
└── main/
    └── pipeline.py              # Orquestrador principal (E → T → L)
```

---

## 🔄 Fluxo do Pipeline

### 1. Extract
Conexão com a Google Drive API via Service Account. Download da planilha RAW exportada como `.xlsx`, lendo cada aba como um DataFrame separado.

### 2. Transform
Cada tabela passa por uma função dedicada de transformação:
- Renomeação de colunas para `snake_case`
- Validação de IDs nulos (Lança erro se encontrar. Isso se faz necessário, uma vez que as tabelas são preenchidas manualmente, por parte do cliente)
- Conversão de tipos (`int64`, `float`, `string`, `datetime`)
- Padronização de strings (Capitalização)
- Strings vazias convertidas para `NaN`

### 3. Model
Após a transformação, cada DataFrame passa pela função de schema que:
- Garante os tipos finais de cada coluna
- Adiciona metadados: `data_ingestao` e `data_processamento`

### 4. Load Incremental
Comparação registro a registro com os dados já existentes no Processed:

| Situação | Ação |
|---|---|
| ➕ Novo ID | INSERT |
| ✏️ ID existente com alteração | UPDATE (sobrescreve) |
| ⏭️ Registro igual | SKIP |
| 🗑️ ID sumiu da fonte | DELETE |

## ⏰ Agendamento

O pipeline é executado automaticamente via **Databricks Jobs** (Serverless), 5x ao dia:

| Job (Pipeline) | Refresh Power BI |
|---|---|
| 08:30 | 08:40 |
| 11:30 | 11:40 |
| 14:30 | 14:40 |
| 16:30 | 16:40 |
| 18:30 | 18:40 |

O refresh do Power BI é disparado automaticamente 10 minutos após cada execução via **Power Automate**.

---

## 🔐 Configuração de Credenciais

As credenciais da Service Account são armazenadas com segurança via **Databricks Secrets**:

```python
dbutils.secrets.get(scope="gcp-credentials", key="private_key")
```

**Obs.** Nunca exponha credenciais diretamente no seu código.

---

## 📊 Dashboard

Dashboard financeiro desenvolvido no Power BI com os seguintes painéis:

- **Indicadores principais** — Faturamento, Despesas, Saldo do período, Ticket Médio, Contas a Pagar
- **Fluxo de Caixa Diário** — Entradas e saídas por dia
- **Receitas por Categoria** — Discriminação de faturamento
- **Despesas por Categoria** — Onde o dinheiro saiu
- **Saldo por Conta/Banco** — Visão consolidada por origem
- **Formas de Pagamento** — Distribuição por tipo
- **Últimas Contas a Pagar** — Tabela com situação e vencimento

---

## ▶️ Como Executar Manualmente

```bash
# Pipeline principal (E → T → L de todas as tabelas)
python main/pipeline.py

# Popular dim_tempo (execução standalone, apenas quando necessário)
python transform/tempo.py
```

> ⚠️ O `tempo.py` **não faz parte do pipeline principal**. Deve ser rodado manualmente apenas uma vez ou quando precisar estender o intervalo de datas.

---

## 📸 Resultado

![Dashboard Comercial 1](https://github.com/user-attachments/assets/0dfa41f0-1bc8-4bfb-b315-aff3360accd3)

**Observações**
- *⁠TODOS OS DADOS APRESENTADOS ACIMA SÃO FICTÍCIOS E AS IDENTIDADES DOS CLIENTES E DA EMPRESA FORAM PRESERVADAS*

---

## 🧑🏻‍💻 Autor

**William Gesner**  
📧 william.gesner@outlook.com · 🔗 [LinkedIn](https://www.linkedin.com/in/william-gesner/) · 🔗 [GitHub](https://github.com/Williamgesner)
