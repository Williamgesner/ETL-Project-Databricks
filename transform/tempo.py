# Responsável por: Popular a tabela dim_tempo com todas as datas necessárias
# ⚠️ Rodar MANUALMENTE apenas uma vez, ou quando precisar estender o período de datas, alterar na linha 294
# ⚠️ NÃO faz parte do pipeline.py (orquestrador) — execução standalone!

import subprocess
subprocess.run([
    "pip", "install",
    "google-api-python-client",
    "google-auth",
    "openpyxl"
], check=True)

import sys
sys.path.append("/Workspace/Users/william.gesner@outlook.com/ELT-Project-Databricks")

import pandas as pd
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account

from config.settings  import SERVICE_ACCOUNT_INFO, FILE_IDS
from models.dim_tempo import aplicar_schema_dim_tempo

# =====================================================
# CONFIGURAÇÃO
# =====================================================

PROCESSED_FOLDER_ID = "1UL-geHSbquQpPT2PGWPjVHonoYQ9-5QE"

PKS = {
    "dim_tempo": "data_completa",
}

# =====================================================
# FUNÇÕES AUXILIARES — GOOGLE SHEETS
# (mesmo padrão do pipeline.py)
# =====================================================

def get_sheets_service():
    """Retorna o cliente da Sheets API com as credenciais corretas."""
    SCOPES = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
    credentials = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=credentials)

def ler_aba_processed(sheets, spreadsheet_id, nome_aba):
    """Lê uma aba do processed e retorna como DataFrame. Retorna None se não existir."""
    try:
        resultado = sheets.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{nome_aba}!A1:ZZ"
        ).execute()
        valores = resultado.get("values", [])
        if len(valores) <= 1:
            return None
        return pd.DataFrame(valores[1:], columns=valores[0])
    except Exception:
        return None

def garantir_aba(sheets, spreadsheet_id, nome_aba):
    """Cria a aba se não existir."""
    info = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    abas = [s["properties"]["title"] for s in info["sheets"]]
    if nome_aba not in abas:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": nome_aba}}}]}
        ).execute()
        print(f"   ✅ Aba '{nome_aba}' criada!")

def salvar_novos_registros(sheets, spreadsheet_id, df_novos, nome_aba):
    """Salva apenas os registros novos no final da aba."""
    dados = df_novos.astype(str).values.tolist()
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{nome_aba}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": dados}
    ).execute()

def obter_sheet_id(sheets, spreadsheet_id, nome_aba):
    """Retorna o sheetId numérico de uma aba pelo nome."""
    info = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in info["sheets"]:
        if sheet["properties"]["title"] == nome_aba:
            return int(sheet["properties"]["sheetId"])
    raise ValueError(f"❌ Aba '{nome_aba}' não encontrada!")

def carregar_incremental(sheets, spreadsheet_id, df_novo, nome_aba, chave_pk):
    """
    Lógica incremental completa:
    ➕ Novos     → INSERT
    ✏️  Alterados → UPDATE
    ⏭️  Iguais    → SKIP
    🗑️  Deletados → DELETE
    """
    print(f"\n   🔄 Verificando incremento — {nome_aba}...")

    garantir_aba(sheets, spreadsheet_id, nome_aba)
    df_existente = ler_aba_processed(sheets, spreadsheet_id, nome_aba)

    # --------------------------------------------------
    # PRIMEIRA CARGA — aba vazia
    # --------------------------------------------------
    if df_existente is None or df_existente.empty:
        print(f"   ℹ️  Primeira carga! Salvando {len(df_novo)} registros...")
        dados = [df_novo.columns.tolist()] + df_novo.astype(str).values.tolist()
        sheets.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{nome_aba}!A1",
            valueInputOption="RAW",
            body={"values": dados}
        ).execute()
        print(f"   ✅ {len(df_novo)} registros salvos!")
        return

    # --------------------------------------------------
    # CARGAS SEGUINTES — compara registro a registro
    # --------------------------------------------------
    df_existente[chave_pk] = df_existente[chave_pk].astype(str)
    df_novo[chave_pk]      = df_novo[chave_pk].astype(str)

    inseridos   = 0
    atualizados = 0
    pulados     = 0
    colunas     = df_novo.columns.tolist()

    for _, linha_nova in df_novo.iterrows():
        pk_valor = str(linha_nova[chave_pk])
        df_match = df_existente[df_existente[chave_pk] == pk_valor]

        # ➕ NOVO
        if df_match.empty:
            salvar_novos_registros(sheets, spreadsheet_id, linha_nova.to_frame().T, nome_aba)
            inseridos += 1
            continue

        colunas_comparacao = [c for c in colunas if c not in ["data_ingestao", "data_processamento"]]
        linha_existente    = df_match.iloc[0]

        houve_alteracao = any(
            str(linha_nova[col]) != str(linha_existente.get(col, ""))
            for col in colunas_comparacao
            if col in linha_existente.index
        )

        # ✏️ ALTERADO
        if houve_alteracao:
            linha_index  = df_existente[df_existente[chave_pk] == pk_valor].index[0]
            linha_numero = linha_index + 2

            linha_nova["data_processamento"] = datetime.now()
            valores = [[str(v) for v in linha_nova[colunas].values]]

            sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{nome_aba}!A{linha_numero}",
                valueInputOption="RAW",
                body={"values": valores}
            ).execute()
            atualizados += 1
            continue

        # ⏭️ IGUAL
        pulados += 1

    # --------------------------------------------------
    # 🗑️ DELETADOS
    # --------------------------------------------------
    ids_novos      = set(df_novo[chave_pk].astype(str))
    ids_existentes = set(df_existente[chave_pk].astype(str))
    ids_deletados  = ids_existentes - ids_novos

    deletados = 0
    if ids_deletados:
        df_existente_atual = ler_aba_processed(sheets, spreadsheet_id, nome_aba)
        df_existente_atual[chave_pk] = df_existente_atual[chave_pk].astype(str)
        sheet_id = obter_sheet_id(sheets, spreadsheet_id, nome_aba)

        for id_deletado in ids_deletados:
            linha_index  = df_existente_atual[df_existente_atual[chave_pk] == id_deletado].index[0]
            linha_numero = int(linha_index) + 2

            sheets.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{
                    "deleteDimension": {
                        "range": {
                            "sheetId"    : int(sheet_id),
                            "dimension"  : "ROWS",
                            "startIndex" : int(linha_numero - 1),
                            "endIndex"   : int(linha_numero)
                        }
                    }
                }]}
            ).execute()

            df_existente_atual = df_existente_atual[
                df_existente_atual[chave_pk] != id_deletado
            ].reset_index(drop=True)
            deletados += 1

    print(f"   ➕ Inseridos  : {inseridos}")
    print(f"   ✏️  Atualizados: {atualizados}")
    print(f"   ⏭️  Pulados    : {pulados}")
    print(f"   🗑️  Deletados  : {deletados}")

# =====================================================
# TRANSFORM — DIM_TEMPO
# =====================================================

def transformar_tempo(data_inicio: str = '2026-01-01', data_fim: str = '2028-12-31') -> pd.DataFrame:
    """
    Gera o DataFrame da dimensão tempo com todas as datas no intervalo.

    Args:
        data_inicio: Data inicial no formato 'YYYY-MM-DD'
        data_fim   : Data final   no formato 'YYYY-MM-DD'

    Returns:
        DataFrame com todos os atributos de data
    """

    print(f"🔄 Transformando Tempo ({data_inicio} → {data_fim})...")

    # =====================================================
    # 1. GERAR TODAS AS DATAS DO PERÍODO
    # =====================================================
    datas = pd.date_range(start=data_inicio, end=data_fim, freq='D')

    # =====================================================
    # 2. DICIONÁRIOS DE APOIO (PT-BR)
    # =====================================================
    nomes_meses = {
        1: 'Janeiro',   2: 'Fevereiro', 3: 'Março',    4: 'Abril',
        5: 'Maio',      6: 'Junho',     7: 'Julho',    8: 'Agosto',
        9: 'Setembro', 10: 'Outubro',  11: 'Novembro', 12: 'Dezembro'
    }

    nomes_meses_abrev = {
        1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr',
        5: 'Mai', 6: 'Jun', 7: 'Jul', 8: 'Ago',
        9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
    }

    nomes_dias_semana = {
        0: 'Segunda-feira', 1: 'Terça-feira',  2: 'Quarta-feira',
        3: 'Quinta-feira',  4: 'Sexta-feira',  5: 'Sábado', 6: 'Domingo'
    }

    # =====================================================
    # 3. MONTAR REGISTROS
    # =====================================================
    registros = []
    for data in datas:
        registros.append({
            'data_completa'  : data.date(),
            'ano'            : data.year,
            'mes'            : data.month,
            'dia'            : data.day,
            'nome_mes'       : nomes_meses[data.month],
            'nome_mes_abrev' : nomes_meses_abrev[data.month],
            'dia_semana'     : data.weekday(),
            'nome_dia_semana': nomes_dias_semana[data.weekday()],
        })

    df = pd.DataFrame(registros)

    print(f"   ✅ {len(df)} datas geradas")

    return df

# =====================================================
# EXECUÇÃO STANDALONE — DIM_TEMPO
# =====================================================

def rodar_dim_tempo():
    print("\n" + "=" * 60)
    print("📅 POPULANDO DIM_TEMPO")
    print("=" * 60)

    inicio = datetime.now()

    sheets         = get_sheets_service()
    spreadsheet_id = FILE_IDS["processed"]

    # 1. TRANSFORM
    df_tempo = transformar_tempo(
        data_inicio='2026-01-01',   # ⚠️ Ajuste conforme necessário
        data_fim   ='2028-12-31'    # ⚠️ Ajuste conforme necessário
    )

    # 2. MODEL
    df_tempo = aplicar_schema_dim_tempo(df_tempo)

    # 3. LOAD INCREMENTAL
    carregar_incremental(sheets, spreadsheet_id, df_tempo, "dim_tempo", PKS["dim_tempo"])

    fim     = datetime.now()
    duracao = (fim - inicio).seconds

    print("\n" + "=" * 60)
    print(f"✅ DIM_TEMPO CONCLUÍDA em {duracao}s")
    print(f"📁 Arquivo: Registros_Processed → aba 'dim_tempo'")
    print("=" * 60)

# =====================================================
# EXECUÇÃO
# =====================================================

rodar_dim_tempo()