from abc import ABC, abstractmethod

class IBallMark(ABC):

    @abstractmethod
    def loadStds(self) -> dict[str, int]:
        pass

    @abstractmethod
    def ballToMark(self, ball: str) -> str:
        pass

    @abstractmethod
    def markToBalls(self, mark: str) -> list[str]:
        pass
