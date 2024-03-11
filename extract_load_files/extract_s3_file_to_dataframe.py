import boto3
import pandas as pd
import awswrangler as wr
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType

class ExtractS3FileToDataFrame:
    def __init__(self, config):
        """
        Construtor da classe ExtractToDataFrame.

        Parâmetros:
        - config (dict): Dicionário contendo as configurações do arquivo.
          Chaves esperadas: 'nome_arquivo', 'diretorio_arquivo', 'tipo_arquivo', 'delimitador', 'quantidade_caracteres_linha'.
        """
        self.nome_bucket = config.get('nome_bucket', None)
        if not self.nome_bucket:
            raise ValueError("nome_bucket é um valor obrigatório")
        self.nome_arquivo = config.get('nome_arquivo', None)
        if not self.nome_arquivo:
            raise ValueError("nome_arquivo é um valor obrigatório")
        self.diretorio_arquivo = config.get('diretorio_arquivo', None)
        if not self.diretorio_arquivo:
            raise ValueError("diretorio_arquivo é um valor obrigatório")
        
        tipo_arquivo = config.get('tipo_arquivo', None)
        if tipo_arquivo and tipo_arquivo.upper() in {'DELIMITADO', 'POSICIONAL', 'EBCDIC', 'PARQUET'}:
            self.tipo_arquivo = tipo_arquivo.upper()
        else:
            raise ValueError("tipo_arquivo inválido. Valores válidos são: DELIMITADO, POSICIONAL, EBCDIC ou PARQUET.")
        
        self.delimitador = config.get('delimitador', None)
        self.quantidade_caracteres_linha = config.get('quantidade_caracteres_linha', 0)
        self.estrutura_arquivo = config.get('estrutura_arquivo', None)

        self.ignorar_primeiras_linhas = config.get('ignorar_primeiras_linhas', 0)
        self.ignorar_ultimas_linhas = config.get('ignorar_ultimas_linhas', 0)

        self.posicoes_arquivo = config.get('posicoes_arquivo', None)
        
        self.dict_book_arquivo = config.get('dict_book_arquivo', None)


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
            raise ValueError("Este método só suporta a criação de DataFrame para arquivos do tipo DELIMITADO, POSICIONAL, PARQUET ou EBCDIC.")


    def criar_dataframe_delimitado(self) -> pd.DataFrame:
        
        if not self.delimitador:
            raise ValueError("Delimitador não especificado para o tipo de arquivo DELIMITADO.")
            
        # Caminho completo do arquivo
        caminho_completo = f"s3://{self.nome_bucket}/{self.diretorio_arquivo}/{self.nome_arquivo}"
        
        # Leitura do arquivo e criação do DataFrame
        dataframe = wr.s3.read_csv(caminho_completo, delimiter=self.delimitador, header=None, 
                                   skiprows=self.ignorar_primeiras_linhas, skipfooter=self.ignorar_ultimas_linhas,
                                   engine='python',
                                   names=self.estrutura_arquivo.keys(), dtype=self.estrutura_arquivo)
        
        return dataframe


    def criar_dataframe_posicional(self) -> pd.DataFrame:
        
        if not self.posicoes_arquivo:
            raise ValueError("Posiciones dos atributos no arquivo não especificado para o tipo de arquivo POSICIONAL.")
            
        # Caminho completo do arquivo
        caminho_completo = f"s3://{self.nome_bucket}/{self.diretorio_arquivo}/{self.nome_arquivo}"
        
        # Leitura do arquivo e criação do DataFrame
        dataframe = wr.s3.read_fwf(caminho_completo, colspecs=self.posicoes_arquivo, 
                                   skiprows=self.ignorar_primeiras_linhas, skipfooter=self.ignorar_ultimas_linhas,
                                   engine='python',
                                   names=self.estrutura_arquivo.keys(), dtype=self.estrutura_arquivo)
        
        return dataframe


    def criar_dataframe_parquet(self) -> pd.DataFrame:
            
        # Caminho completo do arquivo
        caminho_completo = f"s3://{self.nome_bucket}/{self.diretorio_arquivo}/{self.nome_arquivo}"
        
        # Leitura do arquivo e criação do DataFrame
        dataframe = wr.s3.read_parquet(caminho_completo,
                                       columns=self.estrutura_arquivo.keys())
        
        return dataframe


    def criar_dataframe_ebcdic(self) -> pd.DataFrame:

        if not self.quantidade_caracteres_linha:
            raise ValueError("Quantidade de caracteres por linha não especificado para o tipo de arquivo EBCDIC.")


        # Caminho completo do arquivo
        caminho_completo = f"{self.diretorio_arquivo}/{self.nome_arquivo}"

        parser = Parser(self.dict_book_arquivo, ParseType.BINARY_EBCDIC).build()

        arquivo_convertido = []

        client_s3 = boto3.client('s3')
        tamanho_arquivo = client_s3.head_object(Bucket=self.nome_bucket, Key=caminho_completo)['ContentLength']
        ultimo_byte_para_ler = tamanho_arquivo - (self.ignorar_ultimas_linhas * self.quantidade_caracteres_linha)
        quantidade_bytes_lidos = 0

        arquivo = client_s3.get_object(Bucket=self.nome_bucket, Key=caminho_completo)
        
        if self.ignorar_primeiras_linhas:
            arquivo['Body'].read(self.quantidade_caracteres_linha * self.ignorar_primeiras_linhas)
            quantidade_bytes_lidos = self.quantidade_caracteres_linha * self.ignorar_primeiras_linhas
    
        while True:

            conteudo_linha = arquivo['Body'].read(self.quantidade_caracteres_linha)
            quantidade_bytes_lidos += self.quantidade_caracteres_linha
            if not conteudo_linha or quantidade_bytes_lidos > ultimo_byte_para_ler:
                break

            parser.parse(conteudo_linha)
            dict_conteudo_linha = parser.value
            arquivo_convertido.append(dict_conteudo_linha)
        
        dataframe = pd.DataFrame(arquivo_convertido).astype(self.estrutura_arquivo)

        return dataframe
    
    def converte_datetime(self, df: pd.DataFrame, lista_campos_data:list) -> None:

        for campo_data in lista_campos_data:
            df[campo_data[0]] = pd.to_datetime(df[campo_data[0]], format=campo_data[1])
