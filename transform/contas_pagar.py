import pandas as pd

# =====================================================
# TRANSFORMAÇÃO — CONTAS A PAGAR
# =====================================================

def transformar_contas_pagar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a aba 'Contas a Pagar' do Google Sheets.

    Regras aplicadas:
    - Renomeia colunas para snake_case
    - Valida IDs nulos (lança erro se encontrar)
    - Converte tipos corretos
    - Padroniza strings (Primeira letra maiúscula)
    - Converte strings vazias para NaN
    """

    print("🔄 Transformando Contas a Pagar...")

    df = df.copy()

    # =====================================================
    # 1. REMOVER COLUNAS DESNECESSÁRIAS
    # =====================================================
    print("   • Removendo colunas desnecessárias...")
    
    colunas_remover = [
        "categoria_contas_pagar",
        "nome_contato",
        ]
    df = df.drop(columns=[col for col in colunas_remover if col in df.columns])

    # =====================================================
    # 2. RENOMEAR COLUNAS (snake_case padronizado)
    # =====================================================
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # =====================================================
    # 3. VALIDAR IDs NULOS
    # =====================================================
    if df["contas_pagar_id"].isnull().any():
        raise ValueError("❌ ERRO: Existem valores nulos! Corrija na planilha antes de continuar.") # Colunas de ID que não podem ser nulas

    # =====================================================
    # 4. CONVERTER TIPOS
    # =====================================================
    df["contas_pagar_id"] = df["contas_pagar_id"].astype("int64") # contas_pagar_id  → vem como float64 (Excel lê números assim: 1.0, 2.0...), por isso precisa virar int64

    # =====================================================
    # 5. STRINGS VAZIAS → NaN
    # =====================================================
    df["valor"]            = df["valor"].replace("", pd.NA)
    df["situacao"]         = df["situacao"].replace("", pd.NA)
    df["data_vencimento"]  = df["data_vencimento"].replace("", pd.NA)
    df["forma_pagamento"]  = df["forma_pagamento"].replace("", pd.NA)
    df["origem_saida"]     = df["origem_saida"].replace("", pd.NA)
    df["descricao"]        = df["descricao"].replace("", pd.NA)

    # =====================================================
    # 6. PADRONIZAR STRINGS (Primeira letra maiúscula)
    # =====================================================
    df["descricao"]        = df["descricao"].str.strip().str.capitalize()

    # =====================================================
    # 7. GARANTIR TIPOS FINAIS
    # =====================================================
    df["categoria_id"]     = df["categoria_id"].astype("int64")
    df["valor"]            = df["valor"].astype("float")
    df["situacao"]         = df["situacao"].astype("string")
    df["data_vencimento"]  = pd.to_datetime(df["data_vencimento"]).dt.date
    df["contato_id"]       = df["contato_id"].astype("int64")
    df["forma_pagamento"]  = df["forma_pagamento"].astype("string")
    df["origem_saida"]     = df["origem_saida"].astype("string")
    df["descricao"]        = df["descricao"].astype("string")

    print(f"   ✅ {len(df)} registros transformados")
    print(f"   ✅ Nulos em 'valor':           {df['valor'].isnull().sum()}")
    print(f"   ✅ Nulos em 'situacao':        {df['situacao'].isnull().sum()}")
    print(f"   ✅ Nulos em 'data_vencimento': {df['data_vencimento'].isnull().sum()}")
    print(f"   ✅ Nulos em 'forma_pagamento': {df['forma_pagamento'].isnull().sum()}")
    print(f"   ✅ Nulos em 'origem_saida':    {df['origem_saida'].isnull().sum()}")  
    print(f"   ✅ Nulos em 'descricao':       {df['descricao'].isnull().sum()}")

    return df