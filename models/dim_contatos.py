# Responsável por definir a estrutura de tabelas dim_contatos no schema processed

import pandas as pd
from datetime import datetime

# =====================================================
# MODELO TA TABELA — DIM_CONTATOS
# =====================================================

SCHEMA_DIM_CONTATOS = {
    "contato_id"        : {"tipo": "int64",    "nullable": False, "pk": True,  "fk": None},
    "nome_contato"      : {"tipo": "string",   "nullable": False, "pk": False, "fk": None},
    "cpf_cnpj"          : {"tipo": "string",   "nullable": True,  "pk": False, "fk": None}, # 11 (CPF) ou 14 (CNPJ) dígitos
    "data_ingestao"     : {"tipo": "datetime", "nullable": False, "pk": False, "fk": None},
    "data_processamento": {"tipo": "datetime", "nullable": False, "pk": False, "fk": None},
}

def aplicar_schema_dim_contatos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica o schema final da dim_contatos.
    
    - Garante os tipos finais de cada coluna
    - Adiciona metadados de processamento
    """

    print("🔄 Aplicando schema — dim_contatos...")

    df = df.copy()

    # =====================================================
    # 1. GARANTIR TIPOS FINAIS
    # =====================================================
    df["contato_id"]     = df["contato_id"].astype("int64")
    df["nome_contato"]   = df["nome_contato"].astype("string")
    df["cpf_cnpj"]       = df["cpf_cnpj"].astype("string")

    # =====================================================
    # 2. METADADOS
    # =====================================================
    agora = datetime.now()
    df["data_ingestao"]      = agora
    df["data_processamento"] = agora

    print(f"   ✅ Schema aplicado! {len(df)} registros | Colunas: {list(df.columns)}")

    return df