import subprocess 

# =========================================================================================
# INSTALAÇÃO AUTOMÁTICA DE DEPENDÊNCIAS 
# Isso garante que quando o job rodar automaticamente, as libs sempre estarão instaladas!
# =========================================================================================
subprocess.run([
    "pip", "install",
    "google-api-python-client",
    "google-auth",
    "openpyxl"
], check=True)

import sys
sys.path.append("/Workspace/Users/william.gesner@outlook.com/ELT-Project-Databricks")

import pandas as pd
import io
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

from config.settings          import SERVICE_ACCOUNT_INFO, FILE_IDS
from extract.gdrive_connector import conectar_drive, extrair_todas_as_tabelas
from transform.categorias     import transformar_categorias
from transform.contatos       import transformar_contatos
from models.dim_categorias    import aplicar_schema_dim_categorias
from models.dim_contatos      import aplicar_schema_dim_contatos

# =====================================================
# CONFIGURAÇÃO
# =====================================================

# ID da pasta processed no Google Drive que serão salvos os dados processados
PROCESSED_FOLDER_ID = "1UL-geHSbquQpPT2PGWPjVHonoYQ9-5QE" 

# Chave primária de cada tabela
PKS = {
    "dim_categorias"        :"categoria_id",
    "dim_contatos"          : "contato_id",
    # "fato_contas_pagar"     : "contas_pagar_id",
    # "fato_vendas_servicos"  : "servico_id",
    # "dim_tempo"             : "data_completa",
    # "fato_caixa"            : "origem_saida",
}

# =====================================================
# FUNÇÕES AUXILIARES — GOOGLE SHEETS
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

# =====================================================
# LÓGICA INCREMENTAL
# =====================================================

def obter_sheet_id(sheets, spreadsheet_id, nome_aba):
    """Retorna o sheetId numérico de uma aba pelo nome."""
    info = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in info["sheets"]:
        if sheet["properties"]["title"] == nome_aba:
            return int(sheet["properties"]["sheetId"])  # ✅ int() nativo
    raise ValueError(f"❌ Aba '{nome_aba}' não encontrada!")

def carregar_incremental(sheets, spreadsheet_id, df_novo, nome_aba, chave_pk):
    """
    Lógica incremental completa:
    ➕ Novos     → INSERT
    ✏️  Alterados → UPDATE (sobrescreve)
    ⏭️  Iguais    → SKIP
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

    inseridos  = 0
    atualizados = 0
    pulados    = 0
    colunas    = df_novo.columns.tolist()

    for _, linha_nova in df_novo.iterrows():
        pk_valor   = str(linha_nova[chave_pk])
        df_match   = df_existente[df_existente[chave_pk] == pk_valor]

        # ➕ NOVO — ID não existe no processed
        if df_match.empty:
            salvar_novos_registros(sheets, spreadsheet_id, linha_nova.to_frame().T, nome_aba)
            inseridos += 1
            continue

        # Compara os campos (ignora metadados de controle na comparação)
        colunas_comparacao = [c for c in colunas if c not in ["data_ingestao", "data_processamento"]]
        linha_existente    = df_match.iloc[0]

        houve_alteracao = any(
            str(linha_nova[col]) != str(linha_existente.get(col, ""))
            for col in colunas_comparacao
            if col in linha_existente.index
        )

        # ✏️ ALTERADO — sobrescreve o registro existente
        if houve_alteracao:
            linha_index  = df_existente[df_existente[chave_pk] == pk_valor].index[0]
            linha_numero = linha_index + 2  # +1 cabeçalho, +1 base 1

            linha_nova["data_processamento"] = datetime.now()  # atualiza timestamp
            valores = [[str(v) for v in linha_nova[colunas].values]]

            sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{nome_aba}!A{linha_numero}",
                valueInputOption="RAW",
                body={"values": valores}
            ).execute()
            atualizados += 1
            continue

        # ⏭️ IGUAL — não faz nada
        pulados += 1

    # --------------------------------------------------
    # 🗑️ DELETADOS — existe no processed mas sumiu da fonte
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
            linha_numero = int(linha_index) + 2  # ✅ int() nativo — resolve o int64!

            sheets.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{
                    "deleteDimension": {
                        "range": {
                            "sheetId"    : int(sheet_id),          # ✅ int() nativo
                            "dimension"  : "ROWS",
                            "startIndex" : int(linha_numero - 1),  # ✅ int() nativo
                            "endIndex"   : int(linha_numero)       # ✅ int() nativo
                        }
                    }
                }]}
            ).execute()

            df_existente_atual = df_existente_atual[
                df_existente_atual[chave_pk] != id_deletado
            ].reset_index(drop=True)
            deletados += 1

    print(f"   ➕ Inseridos : {inseridos}")
    print(f"   ✏️  Atualizados: {atualizados}")
    print(f"   ⏭️  Pulados   : {pulados}")
    print(f"   🗑️  Deletados  : {deletados}")

# =====================================================
# PIPELINE PRINCIPAL
# =====================================================

def rodar_pipeline():
    print("\n" + "="*60)
    print("🚀 INICIANDO PIPELINE")
    print("="*60)

    inicio = datetime.now()

    # --------------------------------------------------
    # 1. EXTRACT
    # --------------------------------------------------
    print("\n📥 [1/3] EXTRAÇÃO")
    tabelas = extrair_todas_as_tabelas()
    drive   = conectar_drive()
    sheets  = get_sheets_service()
    spreadsheet_id = FILE_IDS["processed"]

    # --------------------------------------------------
    # 2. TRANSFORM + MODEL
    # --------------------------------------------------
    print("\n🔄 [2/3] TRANSFORMAÇÃO")

    df_categorias = transformar_categorias(tabelas["registros__categorias"])
    df_contatos = transformar_contatos(tabelas["registros__contatos"])
    df_categorias = aplicar_schema_dim_categorias(df_categorias)
    df_contatos = aplicar_schema_dim_contatos(df_contatos)

    # --------------------------------------------------
    # 3. LOAD INCREMENTAL
    # --------------------------------------------------
    print("\n💾 [3/3] LOAD INCREMENTAL")

    carregar_incremental(sheets, spreadsheet_id, df_categorias, "dim_categorias", PKS["dim_categorias"])
    carregar_incremental(sheets, spreadsheet_id, df_contatos, "dim_contatos", PKS["dim_contatos"])

    # --------------------------------------------------
    # RELATÓRIO FINAL
    # --------------------------------------------------
    fim      = datetime.now()
    duracao  = (fim - inicio).seconds

    print("\n" + "="*60)
    print(f"✅ PIPELINE CONCLUÍDO em {duracao}s")
    print(f"📁 Arquivo: Registros_Processed")
    print("="*60)

# =====================================================
# EXECUÇÃO
# =====================================================

rodar_pipeline()
