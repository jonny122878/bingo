from typing import List
from pandas import DataFrame

class DeferAlgorithm:
    """計算拖期"""

    def __init__(self) -> None:
        self._TakeColumns: List[str] = []
        self._DfResult: DataFrame = None
        self._DfExport: DataFrame = None

    @property
    def TakeColumns(self):
        return self._TakeColumns

    @TakeColumns.setter
    def TakeColumns(self, value):
        self._TakeColumns = value

    @property
    def DfResult(self):
        return self._DfResult

    @DfResult.setter
    def DfResult(self, value):
        self._DfResult = value

    @property
    def DfExport(self):
        return self._DfExport

    @DfExport.setter
    def DfExport(self, value):
        self._DfExport = value

    def LoadData(self, inputs: List[str]) -> None:
        pass
