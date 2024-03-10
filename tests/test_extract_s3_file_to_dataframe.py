from numpy import datetime64
import pandas as pd
import pytest
import boto3
from moto import mock_aws
from extract_load_files.extract_s3_file_to_dataframe import ExtractS3FileToDataFrame

BUCKET_NAME = 'bucket_test'

livro_datatypes = {
    "codigo_livro": pd.Int64Dtype(),
    "nome_livro": pd.StringDtype(),
    "nome_autor": pd.StringDtype(),
    "ano_lancamento": pd.Int64Dtype()
}

def valida_conteudo_livros(df:pd.DataFrame):
    # valida schema
    assert df.dtypes.get('codigo_livro') == pd.Int64Dtype()
    assert df.dtypes.get('nome_livro') == pd.StringDtype()
    assert df.dtypes.get('nome_autor') == pd.StringDtype()
    assert df.dtypes.get('ano_lancamento') == pd.Int64Dtype()

    # valida dados
    assert df['codigo_livro'][3] == 4
    assert df['nome_livro'][3] == 'O Mar de Monstros'
    assert df['nome_autor'][3] == 'Rick Riordan'
    assert df['ano_lancamento'][3] == 2006

    # valida quantidade registros
    assert len(df) == 12

@pytest.fixture()
def bucket_mock():
    with mock_aws():

        client_s3 = boto3.client('s3')
        client_s3.create_bucket(Bucket=BUCKET_NAME, CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-1',
        })
        yield


@mock_aws
def test_arquivo_delimitado_sem_header_sem_footer(bucket_mock):

    # carrega massa de teste
    arq_test = open('data/delimitado/livros_sem_header_sem_footer.csv', 'rb')
    conteudo_arq_test = arq_test.read()
    client_s3 = boto3.client('s3')
    client_s3.put_object(Bucket=BUCKET_NAME, Key='data/delimitado/livros_sem_header_sem_footer.csv', Body=conteudo_arq_test)

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros_sem_header_sem_footer.csv',
        'diretorio_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',  # Altere conforme necessário
        'delimitador': ',',
        'estrutura_arquivo': livro_datatypes
    }

    exctract = ExtractS3FileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    print(df.head())
    print(df.info())
    print(df.dtypes)

    valida_conteudo_livros(df)

@mock_aws
def test_arquivo_delimitado_sem_footer(bucket_mock):
    
    # carrega massa de teste
    arq_test = open('data/delimitado/livros_sem_footer.csv', 'rb')
    conteudo_arq_test = arq_test.read()
    client_s3 = boto3.client('s3')
    client_s3.put_object(Bucket=BUCKET_NAME, Key='data/delimitado/livros_sem_footer.csv', Body=conteudo_arq_test)

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros_sem_footer.csv',
        'diretorio_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',  # Altere conforme necessário
        'delimitador': ',',
        'estrutura_arquivo': livro_datatypes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 0
    }
    exctract = ExtractS3FileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)


@mock_aws
def test_arquivo_delimitado(bucket_mock):
    
    # carrega massa de teste
    arq_test = open('data/delimitado/livros.csv', 'rb')
    conteudo_arq_test = arq_test.read()
    client_s3 = boto3.client('s3')
    client_s3.put_object(Bucket=BUCKET_NAME, Key='data/delimitado/livros.csv', Body=conteudo_arq_test)

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros.csv',
        'diretorio_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',  # Altere conforme necessário
        'delimitador': ',',
        'estrutura_arquivo': livro_datatypes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 1
    }
    exctract = ExtractS3FileToDataFrame(configuracoes)
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

@mock_aws
def test_arquivo_posicional_sem_header_sem_footer(bucket_mock):
    
    # carrega massa de teste
    arq_test = open('data/posicional/livros_sem_header_sem_footer.txt', 'rb')
    conteudo_arq_test = arq_test.read()
    client_s3 = boto3.client('s3')
    client_s3.put_object(Bucket=BUCKET_NAME, Key='data/posicional/livros_sem_header_sem_footer.txt', Body=conteudo_arq_test)

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros_sem_header_sem_footer.txt',
        'diretorio_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes,
        'posicoes_arquivo': livro_posicoes
    }
    exctract = ExtractS3FileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

@mock_aws
def test_arquivo_posicional_sem_footer(bucket_mock):
    
    # carrega massa de teste
    arq_test = open('data/posicional/livros_sem_footer.txt', 'rb')
    conteudo_arq_test = arq_test.read()
    client_s3 = boto3.client('s3')
    client_s3.put_object(Bucket=BUCKET_NAME, Key='data/posicional/livros_sem_footer.txt', Body=conteudo_arq_test)

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros_sem_footer.txt',
        'diretorio_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes,
        'posicoes_arquivo': livro_posicoes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 0
    }
    exctract = ExtractS3FileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

@mock_aws
def test_arquivo_posicional(bucket_mock):
    
    # carrega massa de teste
    arq_test = open('data/posicional/livros.txt', 'rb')
    conteudo_arq_test = arq_test.read()
    client_s3 = boto3.client('s3')
    client_s3.put_object(Bucket=BUCKET_NAME, Key='data/posicional/livros.txt', Body=conteudo_arq_test)

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros.txt',
        'diretorio_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes,
        'posicoes_arquivo': livro_posicoes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 1
    }
    exctract = ExtractS3FileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

################################################################################

@mock_aws
def test_arquivo_parquet(bucket_mock):
    
    # carrega massa de teste
    arq_test = open('data/parquet/livros.parquet', 'rb')
    conteudo_arq_test = arq_test.read()
    client_s3 = boto3.client('s3')
    client_s3.put_object(Bucket=BUCKET_NAME, Key='data/parquet/livros.parquet', Body=conteudo_arq_test)

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros.parquet',
        'diretorio_arquivo': 'data/parquet',
        'tipo_arquivo': 'PARQUET',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes
    }
    exctract = ExtractS3FileToDataFrame(configuracoes)
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

@mock_aws
def test_arquivo_ebcdic_sem_header_sem_footer(bucket_mock):

    # carrega massa de teste
    arq_test = open('data/ebcdic/livros_sem_header_sem_footer.cobol', 'rb')
    conteudo_arq_test = arq_test.read()
    client_s3 = boto3.client('s3')
    client_s3.put_object(Bucket=BUCKET_NAME, Key='data/ebcdic/livros_sem_header_sem_footer.cobol', Body=conteudo_arq_test)

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros_sem_header_sem_footer.cobol',
        'diretorio_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC',  # Altere conforme necessário
        'quantidade_caracteres_linha': 209,
        'estrutura_arquivo': livro_datatypes,
        'dict_book_arquivo': dict_book_livros
    }

    exctract = ExtractS3FileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()

    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

@mock_aws
def test_arquivo_ebcdic_sem_footer(bucket_mock):

    # carrega massa de teste
    arq_test = open('data/ebcdic/livros_sem_footer.cobol', 'rb')
    conteudo_arq_test = arq_test.read()
    client_s3 = boto3.client('s3')
    client_s3.put_object(Bucket=BUCKET_NAME, Key='data/ebcdic/livros_sem_footer.cobol', Body=conteudo_arq_test)

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros_sem_footer.cobol',
        'diretorio_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC',  # Altere conforme necessário
        'quantidade_caracteres_linha': 209,
        'estrutura_arquivo': livro_datatypes,
        'dict_book_arquivo': dict_book_livros,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 0
    }

    exctract = ExtractS3FileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()

    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

@mock_aws
def test_arquivo_ebcdic(bucket_mock):

    # carrega massa de teste
    arq_test = open('data/ebcdic/livros.cobol', 'rb')
    conteudo_arq_test = arq_test.read()
    client_s3 = boto3.client('s3')
    client_s3.put_object(Bucket=BUCKET_NAME, Key='data/ebcdic/livros.cobol', Body=conteudo_arq_test)

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros.cobol',
        'diretorio_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC',  # Altere conforme necessário
        'quantidade_caracteres_linha': 209,
        'estrutura_arquivo': livro_datatypes,
        'dict_book_arquivo': dict_book_livros,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 1
    }

    exctract = ExtractS3FileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()

    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    valida_conteudo_livros(df)

################################################################################

def valida_conteudo_filmes(df:pd.DataFrame):
    # valida schema
    assert df.dtypes.get('codigo_filme') == pd.Int64Dtype()
    assert df.dtypes.get('nome_filme') == pd.StringDtype()
    assert df.dtypes.get('data_lancamento') == '<M8[ns]'
    assert df.dtypes.get('data_assistido') == '<M8[ns]'

    # valida dados
    assert df['codigo_filme'][0] == 1
    assert df['nome_filme'][0] == 'O Poderoso Chefao'
    assert df['data_lancamento'][0] == datetime64('1972-07-07')
    assert df['data_assistido'][0] == datetime64('2000-10-15')

    # valida quantidade registros
    assert len(df) == 3

filmes_datatypes = {
    "codigo_filme": pd.Int64Dtype(),
    "nome_filme": pd.StringDtype(),
    "data_lancamento": pd.StringDtype(),
    "data_assistido": pd.StringDtype()
}

@mock_aws
def test_arquivo_delimitado_com_data(bucket_mock):

    # carrega massa de teste
    arq_test = open('data/delimitado/filmes.csv', 'rb')
    conteudo_arq_test = arq_test.read()
    client_s3 = boto3.client('s3')
    client_s3.put_object(Bucket=BUCKET_NAME, Key='data/delimitado/filmes.csv', Body=conteudo_arq_test)

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'filmes.csv',
        'diretorio_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',  # Altere conforme necessário
        'delimitador': ',',
        'estrutura_arquivo': filmes_datatypes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 1,
        'converte_datas': [('data_lancamento', '%Y-%m-%d'), ('data_assistido', '%d.%m.%Y')]
    }

    exctract = ExtractS3FileToDataFrame(configuracoes)
    df = exctract.criar_dataframe()
    # print(df.head())
    # print(df.info())
    # print(df.dtypes)

    exctract.converte_datetime(df=df, lista_campos_data=configuracoes['converte_datas'])


    valida_conteudo_filmes(df)

################################################################################
    
def test_nome_bucket_nao_informado():
    configuracoes = dict()
    with pytest.raises(ValueError) as excinfo:
        ExtractS3FileToDataFrame(configuracoes)
    assert str(excinfo.value) == 'nome_bucket é um valor obrigatório'

def test_nome_arquivo_nao_informado():
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
    }
    with pytest.raises(ValueError) as excinfo:
        ExtractS3FileToDataFrame(configuracoes)
    assert str(excinfo.value) == 'nome_arquivo é um valor obrigatório'

def test_diretorio_arquivo_nao_informado():
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'filmes.csv',
    }
    with pytest.raises(ValueError) as excinfo:
        ExtractS3FileToDataFrame(configuracoes)
    assert str(excinfo.value) == 'diretorio_arquivo é um valor obrigatório'

def test_tipo_arquivo_nao_informado():
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'filmes.csv',
        'diretorio_arquivo': 'xpto'
    }
    with pytest.raises(ValueError) as excinfo:
        ExtractS3FileToDataFrame(configuracoes)
    assert str(excinfo.value) == 'tipo_arquivo inválido. Valores válidos são: DELIMITADO, POSICIONAL, EBCDIC ou PARQUET.'

def test_tipo_arquivo_nao_informado():
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'filmes.csv',
        'diretorio_arquivo': 'xpto'
    }
    with pytest.raises(ValueError) as excinfo:
        ExtractS3FileToDataFrame(configuracoes)
    assert str(excinfo.value) == 'tipo_arquivo inválido. Valores válidos são: DELIMITADO, POSICIONAL, EBCDIC ou PARQUET.'


def test_quantidade_caracteres_linha_nao_informada():

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros.cobol',
        'diretorio_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC'  # Altere conforme necessário
    }

    exctract = ExtractS3FileToDataFrame(configuracoes)
    with pytest.raises(ValueError) as excinfo:
        exctract.criar_dataframe()
    assert str(excinfo.value) == 'Quantidade de caracteres por linha não especificado para o tipo de arquivo EBCDIC.'


def test_posicoes_arquivo_nao_informada():
    
    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros.txt',
        'diretorio_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes,
        'ignorar_primeiras_linhas': 1,
        'ignorar_ultimas_linhas': 1
    }
    exctract = ExtractS3FileToDataFrame(configuracoes)
    with pytest.raises(ValueError) as excinfo:
        exctract.criar_dataframe()
    assert str(excinfo.value) == 'Posiciones dos atributos no arquivo não especificado para o tipo de arquivo POSICIONAL.'

def test_delimitado_nao_informado():

    # executa teste
    configuracoes = {
        'nome_bucket': BUCKET_NAME,
        'nome_arquivo': 'livros_sem_header_sem_footer.csv',
        'diretorio_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',  # Altere conforme necessário
        'estrutura_arquivo': livro_datatypes
    }

    exctract = ExtractS3FileToDataFrame(configuracoes)
    with pytest.raises(ValueError) as excinfo:
        exctract.criar_dataframe()
    assert str(excinfo.value) == 'Delimitador não especificado para o tipo de arquivo DELIMITADO.'