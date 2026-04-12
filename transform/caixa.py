import pandas as pd

# =====================================================
# TRANSFORMAÇÃO — CAIXA
# =====================================================

def transformar_caixa(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a aba 'Caixa' do Google Sheets.

    Regras aplicadas:
    - Renomeia colunas para snake_case
    - Valida IDs nulos (lança erro se encontrar)
    - Converte tipos corretos
    - Padroniza strings (Primeira letra maiúscula)
    - Converte strings vazias para NaN
    """

    print("🔄 Transformando Caixa...")

    df = df.copy()

    # =====================================================
    # 1. RENOMEAR COLUNAS (snake_case padronizado)
    # =====================================================
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # =====================================================
    # 2. VALIDAR IDs NULOS
    # =====================================================
    if df["origem"].isnull().any():
        raise ValueError("❌ ERRO: Existem valores nulos! Corrija na planilha antes de continuar.") # Colunas de ID que não podem ser nulas

    # =====================================================
    # 3. STRINGS VAZIAS → NaN
    # =====================================================
    df["data"]          = df["data"].replace("", pd.NA)
    df["saldo_inicial"] = df["saldo_inicial"].replace("", pd.NA)

    # =====================================================
    # 5. GARANTIR TIPOS FINAIS
    # =====================================================
    df["origem"]        = df["origem"].astype("string")
    df["data"]          = pd.to_datetime(df["data"]).dt.date
    df["saldo_inicial"] = df["saldo_inicial"].astype("float")

    print(f"   ✅ {len(df)} registros transformados")
    print(f"   ✅ Nulos em 'origem': {df['origem'].isnull().sum()}")
    print(f"   ✅ Nulos em 'data': {df['data'].isnull().sum()}")
    print(f"   ✅ Nulos em 'saldo_inicial': {df['saldo_inicial'].isnull().sum()}")

    return df