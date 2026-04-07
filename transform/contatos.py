import pandas as pd

# =====================================================
# TRANSFORMAÇÃO — CONTATOS
# =====================================================

def transformar_contatos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a aba 'Contatos' do Google Sheets.

    Regras aplicadas:
    - Renomeia colunas para snake_case
    - Valida IDs nulos (lança erro se encontrar)
    - Converte tipos corretos
    - Padroniza strings (Primeira letra maiúscula)
    - Converte strings vazias para NaN
    """

    print("🔄 Transformando Contatos...")

    df = df.copy()

    # =====================================================
    # 1. RENOMEAR COLUNAS (snake_case padronizado)
    # =====================================================
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # =====================================================
    # 2. VALIDAR IDs NULOS
    # =====================================================
    if df["contato_id"].isnull().any():
        raise ValueError("❌ ERRO: Existem contato_id nulos! Corrija na planilha antes de continuar.")

    # =====================================================
    # 3. CONVERTER TIPOS
    # =====================================================
    df["contato_id"] = df["contato_id"].astype("int64")

    # =====================================================
    # 4. STRINGS VAZIAS → NaN
    # =====================================================
    df["nome_contato"]  = df["nome_contato"].replace("", pd.NA)
    df["cpf_cnpj"]      = df["cpf_cnpj"].replace("", pd.NA)

    # =====================================================
    # 5. PADRONIZAR STRINGS (Primeira letra maiúscula)
    # =====================================================
    df["nome_contato"]  = df["nome_contato"].str.strip().str.capitalize()

    # =====================================================
    # 6. GARANTIR TIPOS FINAIS
    # =====================================================
    df["nome_contato"]  = df["nome_contato"].astype("string")
    df["cpf_cnpj"]      = df["cpf_cnpj"].astype("string")

    print(f"   ✅ {len(df)} registros transformados")
    print(f"   ✅ Nulos em 'nome_contato': {df['nome_contato'].isnull().sum()}")
    print(f"   ✅ Nulos em 'cpf_cnpj': {df['cpf_cnpj'].isnull().sum()}")

    return df