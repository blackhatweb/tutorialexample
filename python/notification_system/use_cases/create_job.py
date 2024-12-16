# use_cases/create_job.py
import uuid
from interfaces.job_repository import JobRepository
from entities.job import Job

class CreateJobUseCase:
    def __init__(self, job_repository: JobRepository):
        self.job_repository = job_repository

    def execute(self, job_data: dict):
        try:
            job_id = uuid.uuid4()
            job = Job(id = job_id, **job_data)
            self.job_repository.create_job(job)
            return {"status": "success", "job_id": job_id}
        except ValueError as e:
            return {"status": "error", "message": str(e)}