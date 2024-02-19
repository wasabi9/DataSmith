from abc import ABC, abstractmethod

class DataSynth(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def compile(self):
        pass

    @abstractmethod
    def process(self):
        pass

    @abstractmethod
    def clean(self):
        pass
