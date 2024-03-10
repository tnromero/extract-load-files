import pandas as pd
from extract_load_files.extract_local_file_to_dataframe import ExtractLocalFileToDataFrame

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
        'diretorio_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',  # Altere conforme necessário
        'delimitador': ',',
        'estrutura_arquivo': livro_datatypes
    }
    exctract = ExtractLocalFileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)


def test_arquivo_delimitado_sem_footer():
    # Exemplo de uso:
    configuracoes = {
        'nome_arquivo': 'livros_sem_footer.csv',
        'diretorio_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',  # Altere conforme necessário
        'delimitador': ',',
        'estrutura_arquivo': livro_datatypes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 0
    }
    exctract = ExtractLocalFileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)


def test_arquivo_delimitado():
    # Exemplo de uso:
    configuracoes = {
        'nome_arquivo': 'livros.csv',
        'diretorio_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',  # Altere conforme necessário
        'delimitador': ',',
        'estrutura_arquivo': livro_datatypes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 1
    }
    exctract = ExtractLocalFileToDataFrame(configuracoes)
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
        'diretorio_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes,
        'posicoes_arquivo': livro_posicoes
    }
    exctract = ExtractLocalFileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

def test_arquivo_posicional_sem_footer():
    # Exemplo de uso:
    configuracoes = {
        'nome_arquivo': 'livros_sem_footer.txt',
        'diretorio_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes,
        'posicoes_arquivo': livro_posicoes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 0
    }
    exctract = ExtractLocalFileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

def test_arquivo_posicional():
    # Exemplo de uso:
    configuracoes = {
        'nome_arquivo': 'livros.txt',
        'diretorio_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes,
        'posicoes_arquivo': livro_posicoes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 1
    }
    exctract = ExtractLocalFileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

################################################################################

def test_arquivo_parquet():
    configuracoes = {
        'nome_arquivo': 'livros.parquet',
        'diretorio_arquivo': 'data/parquet',
        'tipo_arquivo': 'PARQUET',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes
    }
    exctract = ExtractLocalFileToDataFrame(configuracoes)
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
        'diretorio_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC',  # Altere conforme necessário
        'quantidade_caracteres_linha': 209,
        'estrutura_arquivo': livro_datatypes,
        'dict_book_arquivo': dict_book_livros
    }

    exctract = ExtractLocalFileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()

    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

def test_arquivo_ebcdic_sem_footer():
    configuracoes = {
        'nome_arquivo': 'livros_sem_footer.cobol',
        'diretorio_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC',  # Altere conforme necessário
        'quantidade_caracteres_linha': 209,
        'estrutura_arquivo': livro_datatypes,
        'dict_book_arquivo': dict_book_livros,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 0
    }

    exctract = ExtractLocalFileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()

    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

def test_arquivo_ebcdic():
    configuracoes = {
        'nome_arquivo': 'livros.cobol',
        'diretorio_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC',  # Altere conforme necessário
        'quantidade_caracteres_linha': 209,
        'estrutura_arquivo': livro_datatypes,
        'dict_book_arquivo': dict_book_livros,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 1
    }

    exctract = ExtractLocalFileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()

    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

################################################################################

def valida_conteudo_filmes(df:pd.DataFrame):
    # valida schema
    assert df.dtypes.get('codigo_filme') == 'int64'
    assert df.dtypes.get('nome_filme') == 'object'
    assert df.dtypes.get('data_lancamento') == 'object'

    # valida dados
    assert df['codigo_filme'][0] == 1
    assert df['nome_filme'][0] == 'O Poderoso Chefao'
    assert df['data_lancamento'][0] == '1972-07-07'

    # valida quantidade registros
    assert len(df) == 3

filmes_datatypes = {
    "codigo_filme": int,
    "nome_filme": str,
    "data_lancamento": str,
    "data_assistido": str
}

def test_arquivo_delimitado_com_data():
    configuracoes = {
        'nome_arquivo': 'filmes.csv',
        'diretorio_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',  # Altere conforme necessário
        'delimitador': ',',
        'estrutura_arquivo': filmes_datatypes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 1
    }

    exctract = ExtractLocalFileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_filmes(df)