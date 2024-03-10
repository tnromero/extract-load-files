from abc import abstractmethod, ABC
import pandas as pd


class ExtractFileToDataFrame(ABC):

    @abstractmethod
    def criar_dataframe(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def criar_dataframe_delimitado(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def criar_dataframe_posicional(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def criar_dataframe_ebcdic(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def criar_dataframe_parquet(self) -> pd.DataFrame:
        pass