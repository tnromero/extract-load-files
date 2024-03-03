

import codecs


class ExtractMultiLayout:
    def __init__(self, info_arquivo):

        self.nome_arquivo = info_arquivo.get('nome_arquivo', None)
        self.local_arquivo = info_arquivo.get('local_arquivo', None)

        self.quantidade_caracteres_linha = info_arquivo.get('quantidade_caracteres_linha', 0)
        
        self.delimitador = info_arquivo.get('delimitador', None)

        self.ignorar_primeiras_linhas = info_arquivo.get('ignorar_primeiras_linhas', 0)
        self.ignorar_ultimas_linhas = info_arquivo.get('ignorar_ultimas_linhas', 0)

        tipo_mapeamento_identificador_layout = info_arquivo.get('tipo_mapeamento_identificador_layout', None).upper()
        if tipo_mapeamento_identificador_layout in {'DELIMITADO', 'POSICIONAL', 'EBCDIC'}:
            self.tipo_mapeamento_identificador_layout = tipo_mapeamento_identificador_layout
        else:
            raise ValueError("Tipo de mapeamento identificador layout. Valores válidos são: DELIMITADO, POSICIONAL ou EBCDIC.")
        
        self.posicao_identificador_layout = info_arquivo.get('posicao_identificador_layout', None)
        self.dict_identificador_layout = info_arquivo.get('dict_identificador_layout', None)

    def extrair_layout(self) -> None:
        
        if self.tipo_mapeamento_identificador_layout == 'DELIMITADO':
            try:
                return self.extrair_layout_delimitado()
            except Exception as e:
                raise e
        
        elif self.tipo_mapeamento_identificador_layout == 'POSICIONAL':
            try:
                return self.extrair_layout_posicional()
            except Exception as e:
                raise e

        elif self.tipo_mapeamento_identificador_layout == 'EBCDIC':
            try:
                return self.extrair_layout_ebcdic()
            except Exception as e:
                raise e
            
        else:
            raise ValueError("Este método só suporta a criação de DataFrame para arquivos do tipo DELIMITADO, POSICIONAL ou EBCDIC.")


    def extrair_layout_delimitado(self):
        pass


    def extrair_layout_posicional(self):
        
        caminho_completo = f"{self.local_arquivo}/{self.nome_arquivo}"

        dict_arquivos_separados = dict()
        for chave in self.dict_identificador_layout.keys():
            dict_arquivos_separados[chave] = ''
        
        with open(caminho_completo, 'r') as arquivo:
            while True:
                linha = arquivo.readline()
                if not linha:
                    break
                
                identificador = linha[self.posicao_identificador_layout[0]:self.posicao_identificador_layout[1]]
                if identificador in self.dict_identificador_layout.keys():
                    dict_arquivos_separados[identificador] +=linha
                else:
                    raise ValueError(f'Não há referencia para o identificador de layout {identificador}')

        for chave in self.dict_identificador_layout:
            with open(f'{self.local_arquivo}/{self.dict_identificador_layout[chave]}', 'w') as arquivo:
                arquivo.write(dict_arquivos_separados[chave])

    def extrair_layout_ebcdic(self):
        
        caminho_completo = f"{self.local_arquivo}/{self.nome_arquivo}"

        dict_arquivos_separados = dict()
        for chave in self.dict_identificador_layout.keys():
            dict_arquivos_separados[chave] = b''
        
        with open(caminho_completo, 'rb') as arquivo:
            while True:
                linha = arquivo.read(self.quantidade_caracteres_linha)
                if not linha:
                    break
                
                linha_ascii = codecs.decode(linha, 'cp500')

                identificador = linha_ascii[self.posicao_identificador_layout[0]:self.posicao_identificador_layout[1]]
                if identificador in self.dict_identificador_layout.keys():
                    dict_arquivos_separados[identificador] +=linha
                else:
                    raise ValueError(f'Não há referencia para o identificador de layout {identificador}')

        for chave in self.dict_identificador_layout:
            with open(f'{self.local_arquivo}/{self.dict_identificador_layout[chave]}', 'wb') as arquivo:
                arquivo.write(dict_arquivos_separados[chave])