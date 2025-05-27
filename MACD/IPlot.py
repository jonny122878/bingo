from abc import ABC, abstractmethod


class IPlot(ABC):
    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def plot(self, name):
        pass
