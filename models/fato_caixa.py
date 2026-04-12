# Responsável por definir a estrutura de tabelas fdato_caixa no schema processed

import pandas as pd
from datetime import datetime

# =====================================================
# MODELO TA TABELA — FATO_CAIXA
# =====================================================

SCHEMA_FATO_CAIXA = {
    "origem"            : {"tipo": "string",   "nullable": False, "pk": True,  "fk": None},
    "data"              : {"tipo": "datetime", "nullable": False, "pk": False, "fk": None},
    "saldo_inicial"     : {"tipo": "float",    "nullable": False, "pk": False, "fk": None},
    "data_ingestao"     : {"tipo": "datetime", "nullable": False, "pk": False, "fk": None},
    "data_processamento": {"tipo": "datetime", "nullable": False, "pk": False, "fk": None},
}

def aplicar_schema_fato_caixa(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica o schema final da fato_caixa.
    
    - Garante os tipos finais de cada coluna
    - Adiciona metadados de processamento
    """

    print("🔄 Aplicando schema — fato_caixa...")

    df = df.copy()

    # =====================================================
    # 1. GARANTIR TIPOS FINAIS
    # =====================================================
    df["origem"]         = df["origem"].astype("string")
    df["data"]           = pd.to_datetime(df["data"]).dt.date
    df["saldo_inicial"]  = df["saldo_inicial"].astype("float")

    # =====================================================
    # 2. METADADOS
    # =====================================================
    agora = datetime.now()
    df["data_ingestao"]      = agora
    df["data_processamento"] = agora

    print(f"   ✅ Schema aplicado! {len(df)} registros | Colunas: {list(df.columns)}")

    return df