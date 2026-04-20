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
import time
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

from config.settings              import SERVICE_ACCOUNT_INFO, FILE_IDS
from extract.gdrive_connector     import conectar_drive, extrair_todas_as_tabelas
from transform.categorias         import transformar_categorias
from transform.contatos           import transformar_contatos
from transform.contas_pagar       import transformar_contas_pagar
from transform.vendas_servicos    import transformar_vendas_servicos
from transform.caixa              import transformar_caixa
from transform.metas              import transformar_metas

from models.dim_categorias        import aplicar_schema_dim_categorias
from models.dim_contatos          import aplicar_schema_dim_contatos
from models.fato_contas_pagar     import aplicar_schema_fato_contas_pagar
from models.fato_vendas_servicos  import aplicar_schema_fato_vendas_servicos
from models.fato_caixa            import aplicar_schema_fato_caixa
from models.dim_metas             import aplicar_schema_dim_metas

# =====================================================
# CONFIGURAÇÃO
# =====================================================

# ID da pasta processed no Google Drive que serão salvos os dados processados
PROCESSED_FOLDER_ID = "1UL-geHSbquQpPT2PGWPjVHonoYQ9-5QE"

# Chave primária de cada tabela
PKS = {
    "dim_categorias"        : "categoria_id",
    "dim_contatos"          : "contato_id",
    "fato_contas_pagar"     : "contas_pagar_id",
    "fato_vendas_servicos"  : "servico_id",
    "fato_caixa"            : "origem",
    "dim_metas"             : "data_referencia"
}

# =====================================================
# RETRY COM BACKOFF EXPONENCIAL
# =====================================================

def executar_com_retry(func, max_tentativas=5, espera_inicial=10):
    """
    Executa uma chamada à Sheets API com retry automático em caso de 429.
    A espera dobra a cada tentativa: 10s → 20s → 40s → 80s → 160s
    """
    for tentativa in range(1, max_tentativas + 1):
        try:
            return func()
        except HttpError as e:
            if e.resp.status == 429:
                espera = espera_inicial * (2 ** (tentativa - 1))
                print(f"      ⚠️  Rate limit atingido. Aguardando {espera}s antes de tentar novamente... (tentativa {tentativa}/{max_tentativas})")
                time.sleep(espera)
            else:
                raise  # outros erros sobem normalmente
    raise RuntimeError("❌ Número máximo de tentativas atingido após rate limit.")

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
        resultado = executar_com_retry(lambda: 
            sheets.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{nome_aba}!A1:ZZ"
            ).execute()
        )
        valores = resultado.get("values", [])
        if len(valores) <= 1:
            return None
        return pd.DataFrame(valores[1:], columns=valores[0])
    except Exception:
        return None

def garantir_aba(sheets, spreadsheet_id, nome_aba):
    """Cria a aba se não existir."""
    info = executar_com_retry(lambda:
        sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    )
    abas = [s["properties"]["title"] for s in info["sheets"]]
    if nome_aba not in abas:
        executar_com_retry(lambda:
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": nome_aba}}}]}
            ).execute()
        )
        print(f"   ✅ Aba '{nome_aba}' criada!")

def obter_sheet_id(sheets, spreadsheet_id, nome_aba):
    """Retorna o sheetId numérico de uma aba pelo nome."""
    info = executar_com_retry(lambda:
        sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    )
    for sheet in info["sheets"]:
        if sheet["properties"]["title"] == nome_aba:
            return int(sheet["properties"]["sheetId"])  # ✅ int() nativo   
    raise ValueError(f"❌ Aba '{nome_aba}' não encontrada!")

# =====================================================
# LÓGICA INCREMENTAL — COM BATCH (LOTE)
# =====================================================

def carregar_incremental(sheets, spreadsheet_id, df_novo, nome_aba, chave_pk):
    """
    Lógica incremental com operações em BATCH (Lote) para evitar rate limit:
    ➕ Novos     → INSERT
    ✏️  Alterados → UPDATE (sobrescreve)
    ⏭️  Iguais    → SKIP
    🗑️  Deletados → DELETE
    """
    print(f"\n   🔄 Verificando incremento — {nome_aba}...")

    garantir_aba(sheets, spreadsheet_id, nome_aba)
    df_existente = ler_aba_processed(sheets, spreadsheet_id, nome_aba)
    colunas = df_novo.columns.tolist()

    # --------------------------------------------------
    # PRIMEIRA CARGA — aba vazia
    # --------------------------------------------------
    if df_existente is None or df_existente.empty:
        print(f"   ℹ️  Primeira carga! Salvando {len(df_novo)} registros...")
        dados = [colunas] + df_novo.astype(str).values.tolist()
        executar_com_retry(lambda:
            sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{nome_aba}!A1",
                valueInputOption="RAW",
                body={"values": dados}
            ).execute()
        )
        print(f"   ✅ {len(df_novo)} registros salvos!")
        return

    # --------------------------------------------------
    # CARGAS SEGUINTES — compara e acumula em listas
    # --------------------------------------------------
    df_existente[chave_pk] = df_existente[chave_pk].astype(str)
    df_novo[chave_pk]      = df_novo[chave_pk].astype(str)

    linhas_para_inserir  = []   # acumula linhas novas
    ranges_para_atualizar = []  # acumula valueRanges para batchUpdate

    inseridos   = 0
    atualizados = 0
    pulados     = 0

    colunas_comparacao = [c for c in colunas if c not in ["data_ingestao", "data_processamento"]]

    for _, linha_nova in df_novo.iterrows():
        pk_valor = str(linha_nova[chave_pk])
        df_match = df_existente[df_existente[chave_pk] == pk_valor]

        # ➕ NOVO — ID não existe no processed
        if df_match.empty:
            linhas_para_inserir.append([str(v) for v in linha_nova[colunas].values])
            inseridos += 1
            continue

        # Compara os campos (ignora metadados de controle na comparação)
        linha_existente = df_match.iloc[0]
        houve_alteracao = any(
            str(linha_nova[col]) != str(linha_existente.get(col, ""))
            for col in colunas_comparacao
            if col in linha_existente.index
        )

        # ✏️ ALTERADO — acumula para batch update e sobrescreve o registro existente
        if houve_alteracao:
            linha_index  = df_match.index[0]
            linha_numero = linha_index + 2  # +1 cabeçalho, +1 base 1

            linha_nova = linha_nova.copy()
            linha_nova["data_processamento"] = datetime.now() # atualiza timestamp
            valores = [[str(v) for v in linha_nova[colunas].values]]

            ranges_para_atualizar.append({
                "range" : f"{nome_aba}!A{linha_numero}",
                "values": valores
            })
            atualizados += 1
            continue

        # ⏭️ IGUAL — não faz nada
        pulados += 1

    # --------------------------------------------------
    # DISPARA INSERTS em uma única chamada
    # --------------------------------------------------
    if linhas_para_inserir:
        executar_com_retry(lambda:
            sheets.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{nome_aba}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": linhas_para_inserir}
            ).execute()
        )

    # --------------------------------------------------
    # DISPARA UPDATES em uma única chamada batchUpdate
    # --------------------------------------------------
    if ranges_para_atualizar:
        executar_com_retry(lambda:
            sheets.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "valueInputOption": "RAW",
                    "data": ranges_para_atualizar
                }
            ).execute()
        )

    # -------------------------------------------------------------------------------------------------
    # 🗑️ DELETADOS — acumula e dispara em uma única chamada - existe no processed mas sumiu da fonte
    # -------------------------------------------------------------------------------------------------
    ids_novos      = set(df_novo[chave_pk].astype(str))
    ids_existentes = set(df_existente[chave_pk].astype(str))
    ids_deletados  = ids_existentes - ids_novos

    deletados = 0
    if ids_deletados:
        df_atual = ler_aba_processed(sheets, spreadsheet_id, nome_aba)
        df_atual[chave_pk] = df_atual[chave_pk].astype(str)
        sheet_id = obter_sheet_id(sheets, spreadsheet_id, nome_aba)

        # Ordena decrescente para deletar de baixo pra cima
        # (evita que os índices se desloquem durante a deleção)
        linhas_deletar = sorted([
            int(df_atual[df_atual[chave_pk] == id_del].index[0]) + 1  # +1 pelo cabeçalho
            for id_del in ids_deletados
        ], reverse=True)

        requests_delete = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId"   : int(sheet_id),
                        "dimension" : "ROWS",
                        "startIndex": int(linha),
                        "endIndex"  : int(linha) + 1
                    }
                }
            }
            for linha in linhas_deletar
        ]

        executar_com_retry(lambda:
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests_delete}
            ).execute()
        )
        deletados = len(ids_deletados)

    print(f"   ➕ Inseridos  : {inseridos}")
    print(f"   ✏️  Atualizados: {atualizados}")
    print(f"   ⏭️  Pulados    : {pulados}")
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

    df_categorias      = transformar_categorias(tabelas["registros__categorias"])
    df_contatos        = transformar_contatos(tabelas["registros__contatos"])
    df_contas_pagar    = transformar_contas_pagar(tabelas["registros__contas_a_pagar"])
    df_vendas_servicos = transformar_vendas_servicos(tabelas["registros__vendas_de_servicos"])
    df_caixa           = transformar_caixa(tabelas["registros__caixa"])
    df_metas           = transformar_metas(tabelas["registros__metas"])

    df_categorias      = aplicar_schema_dim_categorias(df_categorias)
    df_contatos        = aplicar_schema_dim_contatos(df_contatos)
    df_contas_pagar    = aplicar_schema_fato_contas_pagar(df_contas_pagar)
    df_vendas_servicos = aplicar_schema_fato_vendas_servicos(df_vendas_servicos)
    df_caixa           = aplicar_schema_fato_caixa(df_caixa)
    df_metas           = aplicar_schema_dim_metas(df_metas)

    # --------------------------------------------------
    # 3. LOAD INCREMENTAL
    # --------------------------------------------------
    print("\n💾 [3/3] LOAD INCREMENTAL")

    carregar_incremental(sheets, spreadsheet_id, df_categorias,      "dim_categorias",       PKS["dim_categorias"])
    carregar_incremental(sheets, spreadsheet_id, df_contatos,        "dim_contatos",         PKS["dim_contatos"])
    carregar_incremental(sheets, spreadsheet_id, df_contas_pagar,    "fato_contas_pagar",    PKS["fato_contas_pagar"])
    carregar_incremental(sheets, spreadsheet_id, df_vendas_servicos, "fato_vendas_servicos", PKS["fato_vendas_servicos"])
    carregar_incremental(sheets, spreadsheet_id, df_caixa,           "fato_caixa",           PKS["fato_caixa"])
    carregar_incremental(sheets, spreadsheet_id, df_metas,           "dim_metas",            PKS["dim_metas"])

    # --------------------------------------------------
    # RELATÓRIO FINAL
    # --------------------------------------------------
    fim     = datetime.now()
    duracao = (fim - inicio).seconds

    print("\n" + "="*60)
    print(f"✅ PIPELINE CONCLUÍDO em {duracao}s")
    print(f"📁 Arquivo: Registros_Processed")
    print("="*60)

# =====================================================
# EXECUÇÃO
# =====================================================

rodar_pipeline()