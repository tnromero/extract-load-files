import pandas as pd
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType

class ExtractToDataFrame:
    def __init__(self, config_dict):
        """
        Construtor da classe ExtractToDataFrame.

        Parâmetros:
        - config_dict (dict): Dicionário contendo as configurações do arquivo.
          Chaves esperadas: 'nome_arquivo', 'local_arquivo', 'tipo_arquivo', 'delimitador', 'quantidade_caracteres_linha'.
        """
        self.nome_arquivo = config_dict.get('nome_arquivo', None)
        self.local_arquivo = config_dict.get('local_arquivo', None)
        
        tipo_arquivo = config_dict.get('tipo_arquivo', None).upper()
        if tipo_arquivo in {'DELIMITADO', 'POSICIONAL', 'EBCDIC', 'PARQUET', 'AVRO'}:
            self.tipo_arquivo = tipo_arquivo
        else:
            raise ValueError("Tipo de arquivo inválido. Valores válidos são: DELIMITADO, POSICIONAL, EBCDIC, PARQUET ou AVRO.")
        
        self.delimitador = config_dict.get('delimitador', None)
        self.quantidade_caracteres_linha = config_dict.get('quantidade_caracteres_linha', 0)
        self.estrutura_arquivo = config_dict.get('estrutura_arquivo', None)

        self.ignorar_primeiras_linhas = config_dict.get('ignorar_primeiras_linhas', 0)
        self.ignorar_ultimas_linhas = config_dict.get('ignorar_ultimas_linhas', 0)

        self.posicoes_arquivo = config_dict.get('posicoes_arquivo', None)
        self.dict_book_arquivo = config_dict.get('dict_book_arquivo', None)
        
        

    def criar_dataframe(self) -> pd.DataFrame:
        """
        Método para criar um DataFrame lendo um arquivo delimitado.

        Retorna:
        - DataFrame: DataFrame Pandas criado a partir do arquivo.
        """
        if self.tipo_arquivo == 'DELIMITADO':
            try:
                return self.criar_dataframe_delimitado()
            except Exception as e:
                raise e
        
        elif self.tipo_arquivo == 'POSICIONAL':
            try:
                return self.criar_dataframe_posicional()
            except Exception as e:
                raise e
        
        elif self.tipo_arquivo == 'PARQUET':
            try:
                return self.criar_dataframe_parquet()
            except Exception as e:
                raise e

        elif self.tipo_arquivo == 'EBCDIC':
            try:
                return self.criar_dataframe_ebcdic()
            except Exception as e:
                raise e
            
        else:
            raise ValueError("Este método só suporta a criação de DataFrame para arquivos do tipo DELIMITADO, POSICIONAL")

    def criar_dataframe_delimitado(self) -> pd.DataFrame:
        
        if not self.delimitador:
            raise ValueError("Delimitador não especificado para o tipo de arquivo DELIMITADO.")
            
        # Caminho completo do arquivo
        caminho_completo = f"{self.local_arquivo}/{self.nome_arquivo}"
        
        # Leitura do arquivo e criação do DataFrame
        dataframe = pd.read_csv(caminho_completo, delimiter=self.delimitador, header=None, 
                                skiprows=self.ignorar_primeiras_linhas, skipfooter=self.ignorar_ultimas_linhas,
                                engine='python',
                                names=self.estrutura_arquivo.keys(), dtype=self.estrutura_arquivo)
        
        return dataframe

    def criar_dataframe_posicional(self) -> pd.DataFrame:
        
        if not self.posicoes_arquivo:
            raise ValueError("Posiciones dos atributos no arquivo não especificado para o tipo de arquivo POSICIONAL.")
            
        # Caminho completo do arquivo
        caminho_completo = f"{self.local_arquivo}/{self.nome_arquivo}"
        
        # Leitura do arquivo e criação do DataFrame
        dataframe = pd.read_fwf(caminho_completo, colspecs=self.posicoes_arquivo, 
                                skiprows=self.ignorar_primeiras_linhas, skipfooter=self.ignorar_ultimas_linhas,
                                engine='python',
                                names=self.estrutura_arquivo.keys(), dtype=self.estrutura_arquivo)
        
        return dataframe

    def criar_dataframe_parquet(self) -> pd.DataFrame:
            
        # Caminho completo do arquivo
        caminho_completo = f"{self.local_arquivo}/{self.nome_arquivo}"
        
        # Leitura do arquivo e criação do DataFrame
        dataframe = pd.read_parquet(caminho_completo,
                                    engine='pyarrow', columns=self.estrutura_arquivo.keys())
        
        return dataframe
    
    def criar_dataframe_ebcdic(self) -> pd.DataFrame:

        if not self.quantidade_caracteres_linha:
            raise ValueError("Quantidade de caracteres por linha não especificado para o tipo de arquivo EBCDIC.")


        # Caminho completo do arquivo
        caminho_completo = f"{self.local_arquivo}/{self.nome_arquivo}"

        parser = Parser(self.dict_book_arquivo, ParseType.BINARY_EBCDIC).build()

        arquivo_convertido = []

        with open(caminho_completo, 'rb') as arquivo:
            if self.ignorar_primeiras_linhas:
                arquivo.read(self.quantidade_caracteres_linha * self.ignorar_primeiras_linhas)
        
            while True:

                conteudo_linha = arquivo.read(self.quantidade_caracteres_linha)
                if not conteudo_linha:
                    break

                parser.parse(conteudo_linha)
                dict_conteudo_linha = parser.value
                arquivo_convertido.append(dict_conteudo_linha)
        
        dataframe = pd.DataFrame(arquivo_convertido).astype(self.estrutura_arquivo)


        return dataframe