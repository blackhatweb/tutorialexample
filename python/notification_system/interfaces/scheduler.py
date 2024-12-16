# interfaces/scheduler.py
from abc import ABC, abstractmethod
from datetime import datetime

class Scheduler(ABC):
    @abstractmethod
    def add_job(self, func, trigger, job_id, **kwargs):
        pass

    @abstractmethod
    def reschedule_job(self, job_id, trigger, **kwargs):
        pass

    @abstractmethod
    def remove_job(self, job_id):
        pass
    @abstractmethod
    def get_job(self, job_id):
        pass
    @abstractmethod
    def start(self):
        pass