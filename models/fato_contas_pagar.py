# Responsável por definir a estrutura de tabelas fato_contas_pagar no schema processed

import pandas as pd
from datetime import datetime

# =====================================================
# MODELO TA TABELA — FATO_CATAS_PAGAR
# =====================================================

SCHEMA_FATO_CATAS_PAGAR = {
    "contas_pagar_id"   : {"tipo": "int64",     "nullable": False, "pk": True,  "fk": None},
    "categoria_id"      : {"tipo": "ins64",     "nullable": False, "pk": False, "fk": None},
    "valor"             : {"tipo": "float",     "nullable": True, "pk": False, "fk": None},
    "situacao"          : {"tipo": "string",    "nullable": True, "pk": False, "fk": None},
    "data_vencimento"   : {"tipo": "datetime",  "nullable": True, "pk": False, "fk": None},
    "contato_id"        : {"tipo": "int64",     "nullable": False, "pk": False, "fk": None},
    "forma_pagamento"   : {"tipo": "string",    "nullable": True, "pk": False, "fk": None},
    "origem_saida"      : {"tipo": "string",    "nullable": True, "pk": False, "fk": None},
    "descricao"         : {"tipo": "string",    "nullable": True, "pk": False, "fk": None},
    "data_ingestao"     : {"tipo": "datetime",  "nullable": False, "pk": False, "fk": None},
    "data_processamento": {"tipo": "datetime",  "nullable": False, "pk": False, "fk": None},
}

def aplicar_schema_fato_contas_pagar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica o schema final de fato_contas_pagar.
    
    - Garante os tipos finais de cada coluna
    - Adiciona metadados de processamento
    """

    print("🔄 Aplicando schema — fato_contas_pagar...")

    df = df.copy()

    # =====================================================
    # 1. GARANTIR TIPOS FINAIS
    # =====================================================
    df["contas_pagar_id"]  = df["contas_pagar_id"].astype("int64")
    df["categoria_id"]     = df["categoria_id"].astype("int64")
    df["valor"]            = df["valor"].astype("float")
    df["situacao"]         = df["situacao"].astype("string")
    df["data_vencimento"]  = pd.to_datetime(df["data_vencimento"]).dt.date
    df["contato_id"]       = df["contato_id"].astype("int64")
    df["forma_pagamento"]  = df["forma_pagamento"].astype("string")
    df["origem_saida"]     = df["origem_saida"].astype("string")
    df["descricao"]        = df["descricao"].astype("string")

    # =====================================================
    # 2. METADADOS
    # =====================================================
    agora = datetime.now()
    df["data_ingestao"]      = agora
    df["data_processamento"] = agora

    print(f"   ✅ Schema aplicado! {len(df)} registros | Colunas: {list(df.columns)}")

    return df