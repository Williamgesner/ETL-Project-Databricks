import pandas as pd

# =====================================================
# TRANSFORMAÇÃO — VENDAS DE SERVIÇOS
# =====================================================

def transformar_vendas_servicos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a aba 'Vendas se Serviços' do Google Sheets.

    Regras aplicadas:
    - Renomeia colunas para snake_case
    - Valida IDs nulos (lança erro se encontrar)
    - Converte tipos corretos
    - Padroniza strings (Primeira letra maiúscula)
    - Converte strings vazias para NaN
    """

    print("🔄 Transformando Vendas se Serviços...")

    df = df.copy()

    # =====================================================
    # 1. REMOVER COLUNAS DESNECESSÁRIAS
    # =====================================================
    print("   • Removendo colunas desnecessárias...")
    
    colunas_remover = [
        "categoria_receita",
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
    if df["servico_id"].isnull().any():
        raise ValueError("❌ ERRO: Existem valores nulos! Corrija na planilha antes de continuar.") # Colunas de ID que não podem ser nulas

    # =====================================================
    # 4. CONVERTER TIPOS
    # =====================================================
    df["servico_id"] = df["servico_id"].astype("int64") # servico_id  → vem como float64 (Excel lê números assim: 1.0, 2.0...), por isso precisa virar int64

    # =====================================================
    # 5. STRINGS VAZIAS → NaN
    # =====================================================
    df["valor_total"]   = df["valor_total"].replace("", pd.NA)
    df["data_servico"]  = df["data_servico"].replace("", pd.NA)
    df["descricao"]     = df["descricao"].replace("", pd.NA)

    # =====================================================
    # 6. PADRONIZAR STRINGS (Primeira letra maiúscula)
    # =====================================================
    df["descricao"]     = df["descricao"].str.strip().str.capitalize()

    # =====================================================
    # 7. GARANTIR TIPOS FINAIS
    # =====================================================
    df["categoria_id"]     = df["categoria_id"].astype("int64")
    df["cliente_id"]       = df["cliente_id"].astype("int64")
    df["valor_total"]      = df["valor_total"].astype("float")
    df["origem_entrada"]   = df["origem_entrada"].astype("string")
    df["data_servico"]     = pd.to_datetime(df["data_servico"]).dt.date
    df["descricao"]        = df["descricao"].astype("string")

    print(f"   ✅ {len(df)} registros transformados")
    print(f"   ✅ Nulos em 'valor_total':           {df['valor_total'].isnull().sum()}")
    print(f"   ✅ Nulos em 'data_servico':        {df['data_servico'].isnull().sum()}")
    print(f"   ✅ Nulos em 'descricao':       {df['descricao'].isnull().sum()}")

    return df