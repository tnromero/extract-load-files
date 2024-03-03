

class ExtractPartition:
    def __init__(self, config_dict):
        self.nome_arquivo = config_dict.get('nome_arquivo', None)
        self.local_arquivo = config_dict.get('local_arquivo', None)
        
        self.info_particao = config_dict.get('info_particao', None)
        self.info_particao['local_particao'] = self.info_particao['local_particao'].upper()
        if self.info_particao['local_particao'] not in {'NOME_ARQUIVO', 'HEADER'}:
            raise ValueError("Local da partição inválido. Valores válidos são: NOME_ARQUIVO ou HEADER.")
        
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
        elif self.info_particao['local_particao'] == 'HEADER':
            try:
                return self.extrair_particao_header()
            except Exception as e:
                raise e
        else:
            raise ValueError("Este método só suporta a extração de partição do NOME_ARQUIVO ou HEADER")


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


    def extrair_particao_header(self) -> list:
        
        valor_particao = []

        caminho_completo = f"{self.local_arquivo}/{self.nome_arquivo}"

        header = ''
        with open(caminho_completo, 'r') as arquivo:
            header = arquivo.readline()
        header = header.replace('\r','')
        header = header.replace('\n','')
        
        if self.info_particao['tipo_extracao_particao'] == 'POSICIONAL':
            for posicao in self.info_particao['posicoes_particao']:
                valor_particao.append(header[posicao[0]:posicao[1]])
        
        elif self.info_particao['tipo_extracao_particao'] == 'DELIMITADO':
            
            split_header = header.split(self.info_particao['delimitador'])
            for posicao in self.info_particao['posicoes_particao']:
                valor_particao.append(split_header[posicao])

        return valor_particao