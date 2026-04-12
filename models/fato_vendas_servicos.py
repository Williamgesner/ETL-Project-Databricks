# Responsável por definir a estrutura de tabelas fato_vendas_servicos no schema processed

import pandas as pd
from datetime import datetime

# =====================================================
# MODELO TA TABELA — FATO_VENDAS_SERVICOS
# =====================================================

SCHEMA_FATO_CATAS_PAGAR = {
    "servico_id"        : {"tipo": "int64",     "nullable": False, "pk": True,  "fk": None},
    "categoria_id"      : {"tipo": "ins64",     "nullable": False, "pk": False, "fk": None},
    "cliente_id"        : {"tipo": "ins64",     "nullable": False, "pk": False, "fk": None},
    "valor_total"       : {"tipo": "float",     "nullable": True, "pk": False, "fk": None},
    "origem_entrada"    : {"tipo": "string",    "nullable": False, "pk": False, "fk": None},
    "data_servico"      : {"tipo": "datetime",  "nullable": True, "pk": False, "fk": None},
    "descricao"         : {"tipo": "string",    "nullable": True, "pk": False, "fk": None},
    "data_ingestao"     : {"tipo": "datetime",  "nullable": False, "pk": False, "fk": None},
    "data_processamento": {"tipo": "datetime",  "nullable": False, "pk": False, "fk": None},
}

def aplicar_schema_fato_vendas_servicos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica o schema final de fato_vendas_servicos.
    
    - Garante os tipos finais de cada coluna
    - Adiciona metadados de processamento
    """

    print("🔄 Aplicando schema — fato_vendas_servicos...")

    df = df.copy()

    # =====================================================
    # 1. GARANTIR TIPOS FINAIS
    # =====================================================
    df["servico_id"]     = df["servico_id"].astype("int64")
    df["categoria_id"]   = df["categoria_id"].astype("int64")
    df["cliente_id"]     = df["cliente_id"].astype("int64")
    df["valor_total"]    = df["valor_total"].astype("float")
    df["origem_entrada"] = df["origem_entrada"].astype("string")
    df["data_servico"]     = pd.to_datetime(df["data_servico"]).dt.date
    df["descricao"]      = df["descricao"].astype("string")

    # =====================================================
    # 2. METADADOS
    # =====================================================
    agora = datetime.now()
    df["data_ingestao"]      = agora
    df["data_processamento"] = agora

    print(f"   ✅ Schema aplicado! {len(df)} registros | Colunas: {list(df.columns)}")

    return df