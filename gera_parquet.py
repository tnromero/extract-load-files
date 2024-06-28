import pandas as pd
from extract_load_files.extract_local_file_to_dataframe import ExtractLocalFileToDataFrame

livro_schema = {
    "op": str,
    "operacao_timestamp": str,
    "codigo_livro": int,
    "nome_livro": str,
    "nome_autor": str,
    "ano_lancamento": int
}

configuracoes = {
    'nome_arquivo': 'livros_01.csv',
    'diretorio_arquivo': 'data/cdc-dms',
    'tipo_arquivo': 'DELIMITADO', 
    'delimitador': ',',
    'estrutura_arquivo': livro_schema
}

exctract = ExtractLocalFileToDataFrame(configuracoes)
df = exctract.criar_dataframe()

df.to_parquet('data/cdc-dms/livros_01.parquet')


configuracoes = {
    'nome_arquivo': 'livros_02.csv',
    'diretorio_arquivo': 'data/cdc-dms',
    'tipo_arquivo': 'DELIMITADO', 
    'delimitador': ',',
    'estrutura_arquivo': livro_schema
}

exctract = ExtractLocalFileToDataFrame(configuracoes)
df = exctract.criar_dataframe()
df.to_parquet('data/cdc-dms/livros_02.parquet')

