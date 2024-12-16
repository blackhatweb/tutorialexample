# interfaces/job_repository.py
from abc import ABC, abstractmethod
import uuid
from entities.job import Job  # Giả sử bạn đã có entity Job

class JobRepository(ABC):
    @abstractmethod
    def create_job(self, job: Job) -> uuid.UUID:
        pass

    @abstractmethod
    def get_job_by_id(self, job_id: uuid.UUID) -> Job:
        pass

    @abstractmethod
    def update_job(self, job: Job):
        pass

    @abstractmethod
    def get_all_jobs(self) -> list[Job]:
        pass