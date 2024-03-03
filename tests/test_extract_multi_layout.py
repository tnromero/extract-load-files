from extract_load_files.extract_multi_layout import ExtractMultiLayout


def test_multi_layout_posicional_ascii():
    
    configuracoes = {
        'nome_arquivo': 'contatos_sem_header_sem_footer.txt',
        'local_arquivo': 'data/posicional',
        'tipo_arquivo': 'POSICIONAL',
        'ignorar_primeiras_linhas': 0,
        'ignorar_ultimas_linhas': 0,
        'tipo_mapeamento_identificador_layout': 'POSICIONAL',
        'posicao_identificador_layout': (0, 1),
        'dict_identificador_layout': {
            'A': 'contatos_data_nasc.txt',
            'B': 'contatos_telefone.txt'
        }
    }

    extract = ExtractMultiLayout(configuracoes)
    extract.extrair_layout()


def test_multi_layout_posicional_ebcdic():
    
    configuracoes = {
        'nome_arquivo': 'contatos_sem_header_sem_footer.cobol',
        'local_arquivo': 'data/ebcdic',
        'tipo_arquivo': 'EBCDIC',
        'quantidade_caracteres_linha': 29,
        'ignorar_primeiras_linhas': 0,
        'ignorar_ultimas_linhas': 0,
        'tipo_mapeamento_identificador_layout': 'EBCDIC',
        'posicao_identificador_layout': (0, 1),
        'dict_identificador_layout': {
            'A': 'contatos_data_nasc.cobol',
            'B': 'contatos_telefone.cobol'
        }
    }

    extract = ExtractMultiLayout(configuracoes)
    extract.extrair_layout()