# use_cases/update_job.py
from interfaces.job_repository import JobRepository
from entities.job import Job

class UpdateJobUseCase:
    def __init__(self, job_repository: JobRepository):
        self.job_repository = job_repository

    def execute(self, job: Job):
        try:
            job.validate()  # Validate job data before updating
            self.job_repository.update_job(job)
            return {"status": "success", "message": "Job updated successfully"}
        except ValueError as e:
            return {"status": "error", "message": str(e)}
        except Exception as e: #bắt các exception khác từ repository ví dụ database error
            return {"status": "error", "message": f"Error updating job: {e}"}