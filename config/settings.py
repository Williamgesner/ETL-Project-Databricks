# Credenciais da Service Account e IDs dos arquivos do Google Drive

# Garante que spark e dbutils estão disponíveis em arquivos .py
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

try:
    dbutils
    """
      É uma suíte de ferramentas do Databricks. 
      Permite listar, mover, copiar e deletar arquivos no Databricks File System (DBFS)
      É injetado automaticamente em notebooks, mas em arquivos .py ele precisa ser inicializado manualmente.
    """
except NameError:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

SERVICE_ACCOUNT_INFO = {
  "type": "service_account",
  "project_id": "elt-project-databricks",
  "private_key_id":  dbutils.secrets.get(scope="gcp-credentials", key="private_key_id"),
  "private_key":     dbutils.secrets.get(scope="gcp-credentials", key="private_key"),
  "client_email":    dbutils.secrets.get(scope="gcp-credentials", key="client_email"),
  "client_id":       dbutils.secrets.get(scope="gcp-credentials", key="client_id"),
  "token_uri":      "https://oauth2.googleapis.com/token",  
}

# IDs dos arquivos (pegue da URL do Google Drive de cada arquivo)
# URL exemplo: https://drive.google.com/file/d/ESTE_TRECHO_AQUI/view
FILE_IDS = {
    "registros":    "14eyy7Ylqt_SlzVcTUm9N9FfITdWidOcYEUXVC8r9EgU", # URL da planilha que estamos usando
}
