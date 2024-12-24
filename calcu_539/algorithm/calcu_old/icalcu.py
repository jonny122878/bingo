
class ICalcu(ABC):

    @property
    def includeColumns(self):
        return self._includeColumns

    @includeColumns.setter
    def includeColumns(self, value):
        self._includeColumns = value

    def __init__(self, exportFile: ExportFile, convert: ConvertMark, quantile: Quantile, isToCsv=False, path=None, filename=None) -> None:
        self._exportFile = exportFile
        self._convert = convert
        self._quantile = quantile
        self._isToCsv = isToCsv
        self._path = path
        self._filename = filename
        self._includeColumns = []

    @abstractmethod
    def calcu(self, inputs: List[str]) -> DfInfo:
        pass
