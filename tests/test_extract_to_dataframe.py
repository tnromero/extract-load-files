import pandas as pd
from extract_load_files.extract_to_dataframe import ExtractToDataFrame

livro_datatypes = {
    "codigo_livro": int,
    "nome_livro": str,
    "nome_autor": str,
    "ano_lancamento": int
}

def valida_conteudo_livros(df:pd.DataFrame):
    # valida schema
    assert df.dtypes.get('codigo_livro') == 'int64'
    assert df.dtypes.get('nome_livro') == 'object'
    assert df.dtypes.get('nome_autor') == 'object'
    assert df.dtypes.get('ano_lancamento') == 'int64'

    # valida dados
    assert df['codigo_livro'][0] == 1
    assert df['nome_livro'][0] == 'As Vantanges de Ser Invísivel'
    assert df['nome_autor'][0] == 'Stephen Chbosky'
    assert df['ano_lancamento'][0] == 1999

    # valida quantidade registros
    assert len(df) == 12

def test_arquivo_delimitado_sem_header_sem_footer():
    # Exemplo de uso:
    configuracoes = {
        'nome_arquivo': 'livros_sem_header_sem_footer.csv',
        'local_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',  # Altere conforme necessário
        'delimitador': ',',
        'estrutura_arquivo': livro_datatypes
    }
    exctract = ExtractToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)


def test_arquivo_delimitado_sem_footer():
    # Exemplo de uso:
    configuracoes = {
        'nome_arquivo': 'livros_sem_footer.csv',
        'local_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',  # Altere conforme necessário
        'delimitador': ',',
        'estrutura_arquivo': livro_datatypes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 0
    }
    exctract = ExtractToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)


def test_arquivo_delimitado():
    # Exemplo de uso:
    configuracoes = {
        'nome_arquivo': 'livros.csv',
        'local_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',  # Altere conforme necessário
        'delimitador': ',',
        'estrutura_arquivo': livro_datatypes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 1
    }
    exctract = ExtractToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

################################################################################

livro_posicoes = [
    (0, 5),
    (5, 105),
    (105, 205),
    (205, 209)
]

def test_arquivo_posicional_sem_header_sem_footer():
    # Exemplo de uso:
    configuracoes = {
        'nome_arquivo': 'livros_sem_header_sem_footer.txt',
        'local_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes,
        'posicoes_arquivo': livro_posicoes
    }
    exctract = ExtractToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

def test_arquivo_posicional_sem_footer():
    # Exemplo de uso:
    configuracoes = {
        'nome_arquivo': 'livros_sem_footer.txt',
        'local_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes,
        'posicoes_arquivo': livro_posicoes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 0
    }
    exctract = ExtractToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

def test_arquivo_posicional():
    # Exemplo de uso:
    configuracoes = {
        'nome_arquivo': 'livros.txt',
        'local_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes,
        'posicoes_arquivo': livro_posicoes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 1
    }
    exctract = ExtractToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)


################################################################################

def test_arquivo_parquet():
    configuracoes = {
        'nome_arquivo': 'livros.parquet',
        'local_arquivo': 'data/parquet',
        'tipo_arquivo': 'PARQUET',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes
    }
    exctract = ExtractToDataFrame(configuracoes)
    df = exctract.criar_dataframe()

    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)