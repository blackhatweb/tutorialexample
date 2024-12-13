from abc import ABC, abstractmethod

class MessageQueue(ABC):
    @abstractmethod
    def publish(self, message):
        pass