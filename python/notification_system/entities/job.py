import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPED = "stopped"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class Job:
    id: uuid.UUID
    name: str
    status: JobStatus = JobStatus.PENDING  # Giá trị mặc định là pending
    schedule: str = None # Cron expression
    last_run: datetime = None
    next_run: datetime = None
    message: str = None
    target_audience: str = None
    notification_type: str = None
    created_at: datetime = None
    updated_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.updated_at is None:
            self.updated_at = datetime.now(timezone.utc)
        if not isinstance(self.id, uuid.UUID):
            raise ValueError("Job ID must be a valid UUID")
        if not isinstance(self.status, JobStatus):
            self.status = JobStatus(self.status)

    def validate(self):
        if not self.name:
            raise ValueError("Job name is required")
        if self.schedule is not None and not self.is_valid_cron(self.schedule):
            raise ValueError("Invalid cron expression")
        # Thêm các validation khác nếu cần

    def is_valid_cron(self, cron_string):
        try:
            from croniter import croniter
            croniter(cron_string)
            return True
        except:
            return False

    def start(self):
        if self.status == JobStatus.RUNNING:
            raise ValueError("Job is already running")
        self.status = JobStatus.RUNNING
        self.last_run = datetime.now(timezone.utc)

    def stop(self):
        if self.status == JobStatus.STOPPED:
            raise ValueError("Job is already stopped")
        self.status = JobStatus.STOPPED
        self.next_run = None

    def complete(self):
        self.status = JobStatus.COMPLETED

    def fail(self):
        self.status = JobStatus.FAILED

    def update_next_run(self, next_run: datetime):
        self.next_run = next_run