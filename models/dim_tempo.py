# Responsável por: definir a estrutura da tabela dim_tempo no schema processed

import pandas as pd
from datetime import datetime

# =====================================================
# MODELO TA TABELA — DIM_TEMPO
# =====================================================

SCHEMA_DIM_TEMPO = {
    "data_completa"    : {"tipo": "datetime", "nullable": False, "pk": True,  "fk": None},
    "ano"              : {"tipo": "int64",    "nullable": False, "pk": False, "fk": None},
    "mes"              : {"tipo": "int64",    "nullable": False, "pk": False, "fk": None},
    "dia"              : {"tipo": "int64",    "nullable": False, "pk": False, "fk": None},
    "nome_mes"         : {"tipo": "string",   "nullable": False, "pk": False, "fk": None},
    "nome_mes_abrev"   : {"tipo": "string",   "nullable": False, "pk": False, "fk": None},
    "dia_semana"       : {"tipo": "int64",    "nullable": False, "pk": False, "fk": None},
    "nome_dia_semana"  : {"tipo": "string",   "nullable": False, "pk": False, "fk": None},
}

def aplicar_schema_dim_tempo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica o schema final da dim_tempo.
    
    - Garante os tipos finais de cada coluna
    - Adiciona metadados de processamento
    """

    print("🔄 Aplicando schema — dim_tempo...")

    df = df.copy()

    # =====================================================
    # 1. GARANTIR TIPOS FINAIS
    # =====================================================
    df["data_completa"]   = pd.to_datetime(df["data_completa"])
    df["ano"]             = df["ano"].astype("int64")
    df["mes"]             = df["mes"].astype("int64")
    df["dia"]             = df["dia"].astype("int64")
    df["nome_mes"]        = df["nome_mes"].astype("string")
    df["nome_mes_abrev"]  = df["nome_mes_abrev"].astype("string")
    df["dia_semana"]      = df["dia_semana"].astype("int64")
    df["nome_dia_semana"] = df["nome_dia_semana"].astype("string")

    print(f"   ✅ Schema aplicado! {len(df)} registros | Colunas: {list(df.columns)}")

    return df