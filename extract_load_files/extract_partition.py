import codecs

class ExtractPartition:
    def __init__(self, config_dict):
        self.nome_arquivo = config_dict.get('nome_arquivo', None)
        self.local_arquivo = config_dict.get('local_arquivo', None)
        
        tipo_arquivo = config_dict.get('tipo_arquivo', None).upper()
        if tipo_arquivo in {'DELIMITADO', 'POSICIONAL', 'EBCDIC', 'PARQUET', 'AVRO'}:
            self.tipo_arquivo = tipo_arquivo
        else:
            raise ValueError("Tipo de arquivo inválido. Valores válidos são: DELIMITADO, POSICIONAL, EBCDIC, PARQUET ou AVRO.")
        
        self.quantidade_caracteres_linha = config_dict.get('quantidade_caracteres_linha', 0)

        self.info_particao = config_dict.get('info_particao', None)
        self.info_particao['local_particao'] = self.info_particao['local_particao'].upper()
        if self.info_particao['local_particao'] not in {'NOME_ARQUIVO', 'PRIMEIRA_LINHA'}:
            raise ValueError("Local da partição inválido. Valores válidos são: NOME_ARQUIVO ou PRIMEIRA_LINHA.")
        
        self.info_particao['tipo_extracao_particao'] = self.info_particao['tipo_extracao_particao'].upper()
        if self.info_particao['tipo_extracao_particao'] not in {'POSICIONAL', 'DELIMITADO'}:
            raise ValueError("Tipo de extração da partição inválido. Valores válidos são: POSICIONAL ou DELIMITADO.")
        
        if not self.info_particao['posicoes_particao']:
            raise ValueError("Posições partição inválida. A lista deve conter valores")
        
    
    def extrair_particao(self) -> list:

        if self.info_particao['local_particao'] == 'NOME_ARQUIVO':
            try:
                return self.extrair_particao_nome_arquivo()
            except Exception as e:
                raise e
        elif self.info_particao['local_particao'] == 'PRIMEIRA_LINHA':
            try:
                if self.tipo_arquivo == 'EBCDIC':
                    return self.extrair_particao_primeira_linha_ebcdic()
                else:
                    return self.extrair_particao_primeira_linha_ascii()
            except Exception as e:
                raise e
        else:
            raise ValueError("Este método só suporta a extração de partição do NOME_ARQUIVO ou PRIMEIRA_LINHA")


    def extrair_particao_nome_arquivo(self) -> list:
        
        valor_particao = []

        if self.info_particao['tipo_extracao_particao'] == 'POSICIONAL':
            for posicao in self.info_particao['posicoes_particao']:
                valor_particao.append(self.nome_arquivo[posicao[0]:posicao[1]])
        
        elif self.info_particao['tipo_extracao_particao'] == 'DELIMITADO':
            posicao_ponto = self.nome_arquivo.rfind(".")
            nome_arquivo_sem_extensao = self.nome_arquivo[:posicao_ponto]
            split_nome_arquivo_sem_extensao = nome_arquivo_sem_extensao.split(self.info_particao['delimitador'])
            for posicao in self.info_particao['posicoes_particao']:
                valor_particao.append(split_nome_arquivo_sem_extensao[posicao])
            
        return valor_particao


    def extrair_particao_primeira_linha_ascii(self) -> list:
        
        valor_particao = []

        caminho_completo = f"{self.local_arquivo}/{self.nome_arquivo}"

        with open(caminho_completo, 'r') as arquivo:
            primeira_linha = arquivo.readline()
        primeira_linha = primeira_linha.replace('\r','')
        primeira_linha = primeira_linha.replace('\n','')
        
        if self.info_particao['tipo_extracao_particao'] == 'POSICIONAL':
            for posicao in self.info_particao['posicoes_particao']:
                valor_particao.append(primeira_linha[posicao[0]:posicao[1]])
        
        elif self.info_particao['tipo_extracao_particao'] == 'DELIMITADO':
            
            split_primeira_linha = primeira_linha.split(self.info_particao['delimitador'])
            for posicao in self.info_particao['posicoes_particao']:
                valor_particao.append(split_primeira_linha[posicao])

        return valor_particao

    def extrair_particao_primeira_linha_ebcdic(self) -> list:

        valor_particao = []

        caminho_completo = f"{self.local_arquivo}/{self.nome_arquivo}"

        with open(caminho_completo, 'rb') as arquivo:
            primeira_linha = arquivo.read(self.quantidade_caracteres_linha)
        
        primeira_linha_ascii = codecs.decode(primeira_linha, 'cp500')

        if self.info_particao['tipo_extracao_particao'] == 'POSICIONAL':
            for posicao in self.info_particao['posicoes_particao']:
                valor_particao.append(primeira_linha_ascii[posicao[0]:posicao[1]])
        else:
            raise ValueError('Para arquivos EBCDIC não é possível extrair a partição pelo módulo DELIMITADO')

        return valor_particao