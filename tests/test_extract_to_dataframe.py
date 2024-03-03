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
    assert df['codigo_livro'][3] == 4
    assert df['nome_livro'][3] == 'O Mar de Monstros'
    assert df['nome_autor'][3] == 'Rick Riordan'
    assert df['ano_lancamento'][3] == 2006

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

################################################################################
    
dict_book_livros = {
    'CODIGO_LIVRO': {
        'type': 'NUMERIC',
        'level': 3,
        'name': 'codigo_livro',
        'format': '9(005)',
        'subformat': 'NORMAL',
        'decimals': 0,
        'size': 5,
        'start': 0,
        'occurs': None
    },
    'NOME_LIVRO': {
        'type': 'ALPHANUMERIC',
        'level': 3,
        'name': 'nome_livro',
        'format': 'X(100)',
        'subformat': 'NORMAL',
        'decimals': 0,
        'size': 100,
        'start': 5,
        'occurs': None
    },
    'NOME_AUTOR': {
        'type': 'ALPHANUMERIC',
        'level': 3,
        'name': 'nome_autor',
        'format': 'X(100)',
        'subformat': 'NORMAL',
        'decimals': 0,
        'size': 100,
        'start': 105,
        'occurs': None
    },
    'ANO_LANCAMENTO': {
        'type': 'NUMERIC',
        'level': 3,
        'name': 'ano_lancamento',
        'format': '9(004)',
        'subformat': 'NORMAL',
        'decimals': 0,
        'size': 4,
        'start': 205,
        'occurs': None
    }
}

def test_arquivo_ebcdic_sem_header_sem_footer():
    configuracoes = {
        'nome_arquivo': 'livros_sem_header_sem_footer.cobol',
        'local_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC',  # Altere conforme necessário
        'quantidade_caracteres_linha': 209,
        'estrutura_arquivo': livro_datatypes,
        'dict_book_arquivo': dict_book_livros
    }

    exctract = ExtractToDataFrame(configuracoes)
    df = exctract.criar_dataframe()

    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

def test_arquivo_ebcdic_sem_footer():
    configuracoes = {
        'nome_arquivo': 'livros_sem_footer.cobol',
        'local_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC',  # Altere conforme necessário
        'quantidade_caracteres_linha': 209,
        'estrutura_arquivo': livro_datatypes,
        'dict_book_arquivo': dict_book_livros,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 0
    }

    exctract = ExtractToDataFrame(configuracoes)
    df = exctract.criar_dataframe()

    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

def test_arquivo_ebcdic():
    configuracoes = {
        'nome_arquivo': 'livros.cobol',
        'local_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC',  # Altere conforme necessário
        'quantidade_caracteres_linha': 209,
        'estrutura_arquivo': livro_datatypes,
        'dict_book_arquivo': dict_book_livros,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 1
    }

    exctract = ExtractToDataFrame(configuracoes)
    df = exctract.criar_dataframe()

    print(df.head())
    print(df.info())
    print(df.dtypes)
    print(df['ano_lancamento'][11])

    valida_conteudo_livros(df)