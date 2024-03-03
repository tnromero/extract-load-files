from extract_load_files.extract_partition import ExtractPartition

def test_particao_unica_nome_arquivo_posicional():
    info_particao = {
        'local_particao': 'NOME_ARQUIVO',
        'tipo_extracao_particao': 'POSICIONAL',
        'posicoes_particao': [
            (7, 15),
        ]
    }
    
    configuracoes = {
        'nome_arquivo': 'livros_20240303.csv',
        'tipo_arquivo': 'DELIMITADO',
        'info_particao': info_particao
    }

    extract = ExtractPartition(configuracoes)
    particao = extract.extrair_particao()

    assert particao == ['20240303']


def test_3_particoes_nome_arquivo_posicional():
    info_particao = {
        'local_particao': 'NOME_ARQUIVO',
        'tipo_extracao_particao': 'POSICIONAL',
        'posicoes_particao': [
            (7, 11),
            (11, 13),
            (13, 15),
        ]
    }
    
    configuracoes = {
        'nome_arquivo': 'livros_20240303.csv',
        'tipo_arquivo': 'DELIMITADO',
        'info_particao': info_particao
    }

    extract = ExtractPartition(configuracoes)
    particao = extract.extrair_particao()

    assert particao == ['2024','03','03']


def test_particao_unica_nome_arquivo_delimitado():
    info_particao = {
        'local_particao': 'NOME_ARQUIVO',
        'tipo_extracao_particao': 'DELIMITADO',
        'posicoes_particao': [1],
        'delimitador': '_'
    }
    
    configuracoes = {
        'nome_arquivo': 'livros_20240303.csv',
        'tipo_arquivo': 'DELIMITADO',
        'info_particao': info_particao
    }

    extract = ExtractPartition(configuracoes)
    particao = extract.extrair_particao()

    assert particao == ['20240303']


def test_3_particoes_nome_arquivo_delimitado():
    info_particao = {
        'local_particao': 'NOME_ARQUIVO',
        'tipo_extracao_particao': 'DELIMITADO',
        'posicoes_particao': [1, 2, 3],
        'delimitador': '_'
    }
    
    configuracoes = {
        'nome_arquivo': 'livros_2024_03_03.csv',
        'tipo_arquivo': 'DELIMITADO',
        'info_particao': info_particao
    }

    extract = ExtractPartition(configuracoes)
    particao = extract.extrair_particao()

    assert particao == ['2024','03','03']


def test_particao_unica_header_posicional():
    info_particao = {
        'local_particao': 'PRIMEIRA_LINHA',
        'tipo_extracao_particao': 'POSICIONAL',
        'posicoes_particao': [
            (1, 9),
        ]
    }
    
    configuracoes = {
        'nome_arquivo': 'livros_sem_footer.txt',
        'local_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',
        'info_particao': info_particao
    }

    extract = ExtractPartition(configuracoes)
    particao = extract.extrair_particao()

    assert particao == ['20240302']


def test_3_particoes_header_posicional():
    info_particao = {
        'local_particao': 'PRIMEIRA_LINHA',
        'tipo_extracao_particao': 'POSICIONAL',
        'posicoes_particao': [
            (1, 5),
            (5, 7),
            (7, 9),
        ]
    }
    
    configuracoes = {
        'nome_arquivo': 'livros_sem_footer.txt',
        'local_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',
        'info_particao': info_particao
    }

    extract = ExtractPartition(configuracoes)
    particao = extract.extrair_particao()

    assert particao == ['2024','03','02']


def test_particao_unica_header_delimitado():
    info_particao = {
        'local_particao': 'PRIMEIRA_LINHA',
        'tipo_extracao_particao': 'DELIMITADO',
        'posicoes_particao': [1],
        'delimitador': ','
    }
    
    configuracoes = {
        'nome_arquivo': 'livros_sem_footer.csv',
        'local_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',
        'info_particao': info_particao
    }

    extract = ExtractPartition(configuracoes)
    particao = extract.extrair_particao()

    assert particao == ['20240302']


def test_3_particioes_header_delimitado():
    info_particao = {
        'local_particao': 'PRIMEIRA_LINHA',
        'tipo_extracao_particao': 'DELIMITADO',
        'posicoes_particao': [1,2,3],
        'delimitador': ','
    }
    
    configuracoes = {
        'nome_arquivo': 'livros_sem_footer_subparticionado.csv',
        'local_arquivo': 'data/delimitado',
        'tipo_arquivo': 'DELIMITADO',
        'info_particao': info_particao
    }

    extract = ExtractPartition(configuracoes)
    particao = extract.extrair_particao()

    assert particao == ['2024','03','02']


def test_particao_unica_header_ebcdic():
    info_particao = {
        'local_particao': 'PRIMEIRA_LINHA',
        'tipo_extracao_particao': 'POSICIONAL',
        'posicoes_particao': [
            (1, 9),
        ]
    }
    
    configuracoes = {
        'nome_arquivo': 'livros_sem_footer.cobol',
        'local_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC',
        'quantidade_caracteres_linha': 209,
        'info_particao': info_particao
    }

    extract = ExtractPartition(configuracoes)
    particao = extract.extrair_particao()

    assert particao == ['20240302']


def test_3_particoes_header_ebcdic():
    info_particao = {
        'local_particao': 'PRIMEIRA_LINHA',
        'tipo_extracao_particao': 'POSICIONAL',
        'posicoes_particao': [
            (1, 5),
            (5, 7),
            (7, 9)
        ]
    }
    
    configuracoes = {
        'nome_arquivo': 'livros_sem_footer.cobol',
        'local_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC',
        'quantidade_caracteres_linha': 209,
        'info_particao': info_particao
    }

    extract = ExtractPartition(configuracoes)
    particao = extract.extrair_particao()

    assert particao == ['2024','03','02']