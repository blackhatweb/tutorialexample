# frameworks/apscheduler_adapter.py
from interfaces.scheduler import Scheduler
from apscheduler.schedulers.background import BackgroundScheduler

class APSchedulerAdapter(Scheduler):
    def __init__(self):
        self.scheduler = BackgroundScheduler()

    def add_job(self, func, trigger, job_id, **kwargs):
        self.scheduler.add_job(func=func, trigger=trigger, id=job_id, **kwargs)

    def reschedule_job(self, job_id, trigger, **kwargs):
        self.scheduler.reschedule_job(job_id=job_id, trigger=trigger, **kwargs)

    def remove_job(self, job_id):
        self.scheduler.remove_job(job_id=job_id)

    def get_job(self, job_id):
        return self.scheduler.get_job(job_id=job_id)
    def start(self):
        self.scheduler.start()
    def shutdown(self):
        self.scheduler.shutdown()