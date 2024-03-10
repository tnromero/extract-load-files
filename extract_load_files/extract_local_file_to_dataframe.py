import os
import pandas as pd
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType

from extract_load_files.extract_file_to_data_frame import ExtractFileToDataFrame

class ExtractLocalFileToDataFrame(ExtractFileToDataFrame):
    def __init__(self, config):
        """
        Construtor da classe ExtractToDataFrame.

        Parâmetros:
        - config (dict): Dicionário contendo as configurações do arquivo.
          Chaves esperadas: 'nome_arquivo', 'diretorio_arquivo', 'tipo_arquivo', 'delimitador', 'quantidade_caracteres_linha'.
        """
        self.nome_arquivo = config.get('nome_arquivo', None)
        self.diretorio_arquivo = config.get('diretorio_arquivo', None)
        tipo_local_arquivo = config.get('tipo_local_arquivo', 'LOCAL').upper()
        if tipo_local_arquivo in {'LOCAL', 'S3'}:
            self.tipo_local_arquivo = tipo_local_arquivo
        else:
            raise ValueError("Tipo de arquivo inválido. Valores válidos são: LOCAL ou S3.")
        
        tipo_arquivo = config.get('tipo_arquivo', None).upper()
        if tipo_arquivo in {'DELIMITADO', 'POSICIONAL', 'EBCDIC', 'PARQUET'}:
            self.tipo_arquivo = tipo_arquivo
        else:
            raise ValueError("Tipo de arquivo inválido. Valores válidos são: DELIMITADO, POSICIONAL, EBCDIC ou PARQUET.")
        
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
        caminho_completo = f"{self.diretorio_arquivo}/{self.nome_arquivo}"
        
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
        caminho_completo = f"{self.diretorio_arquivo}/{self.nome_arquivo}"
        
        # Leitura do arquivo e criação do DataFrame
        dataframe = pd.read_fwf(caminho_completo, colspecs=self.posicoes_arquivo, 
                                skiprows=self.ignorar_primeiras_linhas, skipfooter=self.ignorar_ultimas_linhas,
                                engine='python',
                                names=self.estrutura_arquivo.keys(), dtype=self.estrutura_arquivo)
        
        return dataframe


    def criar_dataframe_parquet(self) -> pd.DataFrame:
            
        # Caminho completo do arquivo
        caminho_completo = f"{self.diretorio_arquivo}/{self.nome_arquivo}"
        
        # Leitura do arquivo e criação do DataFrame
        dataframe = pd.read_parquet(caminho_completo,
                                    engine='pyarrow', columns=self.estrutura_arquivo.keys())
        
        return dataframe


    def criar_dataframe_ebcdic(self) -> pd.DataFrame:

        if not self.quantidade_caracteres_linha:
            raise ValueError("Quantidade de caracteres por linha não especificado para o tipo de arquivo EBCDIC.")


        # Caminho completo do arquivo
        caminho_completo = f"{self.diretorio_arquivo}/{self.nome_arquivo}"

        parser = Parser(self.dict_book_arquivo, ParseType.BINARY_EBCDIC).build()

        arquivo_convertido = []

        tamanho_arquivo = os.path.getsize(caminho_completo)
        ultimo_byte_para_ler = tamanho_arquivo - (self.ignorar_ultimas_linhas * self.quantidade_caracteres_linha)
        quantidade_bytes_lidos = 0

        with open(caminho_completo, 'rb') as arquivo:
            if self.ignorar_primeiras_linhas:
                arquivo.read(self.quantidade_caracteres_linha * self.ignorar_primeiras_linhas)
                quantidade_bytes_lidos = self.quantidade_caracteres_linha * self.ignorar_primeiras_linhas
        
            while True:

                conteudo_linha = arquivo.read(self.quantidade_caracteres_linha)
                quantidade_bytes_lidos += self.quantidade_caracteres_linha
                if not conteudo_linha or quantidade_bytes_lidos > ultimo_byte_para_ler:
                    break

                parser.parse(conteudo_linha)
                dict_conteudo_linha = parser.value
                arquivo_convertido.append(dict_conteudo_linha)
        
        dataframe = pd.DataFrame(arquivo_convertido).astype(self.estrutura_arquivo)

        return dataframe