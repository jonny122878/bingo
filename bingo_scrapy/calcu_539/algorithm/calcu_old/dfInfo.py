import pandas as pd


class DfInfo:
    """原始計算、刪除欄位"""

    def __init__(self) -> None:
        self._df = pd.DataFrame()
        self._dfDrop = pd.DataFrame()
        pass

    @property
    def df(self) -> str:
        return self._df

    @df.setter
    def df(self, value: str):
        self._df = value

    @property
    def dfDrop(self) -> str:
        return self._dfDrop

    @dfDrop.setter
    def dfDrop(self, value: str):
        self._dfDrop = value
