# Responsável por: extrair a tabelas do Google Drive

import sys
sys.path.append("/Workspace/Users/william.gesner@outlook.com/ELT-Project-Databricks")
from config.settings import SERVICE_ACCOUNT_INFO, FILE_IDS

import io
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# =====================================================
# CONEXÃO COM O GOOGLE DRIVE
# =====================================================

def conectar_drive():
    """Autentica e retorna o cliente do Google Drive."""
    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
    credentials = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO, scopes=SCOPES
    )
    return build("drive", "v3", credentials=credentials)

def baixar_arquivo(drive_service, file_id):
    """Exporta um Google Sheets como Excel."""
    
    request = drive_service.files().export_media(
        fileId=file_id,
        mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buffer.seek(0)
    return buffer

# =====================================================
# EXTRAÇÃO DAS TABELAS
# =====================================================

def extrair_todas_as_tabelas():
    """Extrai todos os arquivos do Drive e retorna como dicionário de DataFrames."""
    
    drive = conectar_drive()
    tabelas = {}

    print("🔄 Conectando ao Google Drive...")

    # --- Registros.xlsx (várias abas) ---
    print("\n📊 Extraindo Registros.xlsx...")
    buffer = baixar_arquivo(drive, FILE_IDS["registros"])
    abas = pd.read_excel(buffer, sheet_name=None)
    for nome_aba, df in abas.items():
        chave = f"registros__{nome_aba.lower().replace(' ', '_')}"
        tabelas[chave] = df
        print(f"   ✅ Aba '{nome_aba}': {df.shape[0]} linhas x {df.shape[1]} colunas")

    print("\n✅ Extração concluída!")
    return tabelas

# =====================================================
# VISUALIZAÇÃO - PARA TESTE 
# =====================================================

# Roda a extração e exibe todas as tabelas
tabelas = extrair_todas_as_tabelas()

print("\n\n📋 TABELAS DISPONÍVEIS:")
for nome, df in tabelas.items():
    print(f"\n{'='*60}")
    print(f"📌 {nome}")
    # display(tabelas["registros__contas_a_pagar"].head(5)) # Se eu usar display, 100% dos campos precisam estar preenchidos. Se for apenas para visualizar e testar, usar print, pois é normal que algum campo não esteja preenchido por parte do cliente.
    print (tabelas["registros__contas_a_pagar"].head(5))
