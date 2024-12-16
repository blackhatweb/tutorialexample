from flask import Flask, request, jsonify
import uuid
from frameworks.apscheduler_adapter import APSchedulerAdapter
from use_cases.send_notification import SendNotificationUseCase
from frameworks.postgresql_job_repository import PostgreSQLJobRepository
from use_cases.create_job import CreateJobUseCase
from use_cases.update_job import UpdateJobUseCase
from entities.job import Job
from interfaces.scheduler import Scheduler
import json
import psycopg2
#import kết nối rabbitmq
from frameworks.rabbitmq import RabbitMQ

def connect_db(): # Giữ nguyên hàm connect db
    try:
        conn = psycopg2.connect("dbname=your_db user=your_user password=your_password host=your_host port=your_port")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

app = Flask(__name__)

class JobManager:
    def __init__(self, scheduler: Scheduler, send_notifications_use_case: SendNotificationUseCase, job_repository):
        self.scheduler = scheduler
        self.send_notifications_use_case = send_notifications_use_case
        self.job_repository = job_repository
        self.scheduler.start()

    def create_job(self, job_data):
        job = Job(id=uuid.uuid4(), **job_data)
        job.validate()
        job_id = self.job_repository.create_job(job)
        if job_id:
            try:
                self.scheduler.add_job(
                    func=self.execute_job,
                    trigger='cron',
                    id=str(job_id),
                    kwargs = {'job_id': str(job_id)},
                    **parse_cron_string(job.schedule)
                )
                return {"status": "success", "job_id": job_id}
            except ValueError as e:
                return {"status": "error", "message": str(e)}
        return {"status": "error", "message": "can not create job"}


    def execute_job(self, job_id): # Hàm này sẽ được gọi bởi APScheduler
        job = self.job_repository.get_job_by_id(uuid.UUID(job_id))
        if job:
            job.start()
            self.job_repository.update_job(job)
            job_data = {
                "user_id": job.target_audience,
                "message": job.message,
                "notification_type": job.notification_type
            }
            self.send_notifications_use_case.execute(job_data) # Gọi Use Case
            job.complete()
            self.job_repository.update_job(job)
        else:
            print(f"job {job_id} not found")

    def start_job(self, job_id):
        job = self.job_repository.get_job_by_id(uuid.UUID(job_id))
        if job:
            try:
                if self.scheduler.get_job(str(job_id)).next_run is None:
                    self.scheduler.reschedule_job(str(job_id), trigger='cron', **parse_cron_string(job.schedule))
                job.status = "running"
                self.job_repository.update_job(job)
                return {"message":"job started"}
            except Exception as e:
                return {"error": str(e)}
        return {"message":"job not found"}
    def stop_job(self, job_id):
        job = self.job_repository.get_job_by_id(uuid.UUID(job_id))
        if job:
            try:
                self.scheduler.reschedule_job(str(job_id), trigger=None)
                job.status = "stopped"
                self.job_repository.update_job(job)
                return {"message":"job stopped"}
            except Exception as e:
                return {"error": str(e)}
        return {"message":"job not found"}

def parse_cron_string(cron_string): # Giữ nguyên
    parts = cron_string.split()
    if len(parts) != 5:
        raise ValueError("Cron string must have 5 parts")
    return {
        'minute': parts[0],
        'hour': parts[1],
        'day': parts[2],
        'month': parts[3],
        'day_of_week': parts[4]
    }
@app.route('/jobs', methods=['POST'])
def create_job_route():
    conn = connect_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    repo = PostgreSQLJobRepository(conn)
    scheduler = APSchedulerAdapter()
    use_case = SendNotificationUseCase(RabbitMQ())
    job_manager = JobManager(scheduler,use_case, repo)
    data = request.get_json()
    response = job_manager.create_job(data)
    conn.close()
    return jsonify(response), 201 if response["status"] == "success" else 400

@app.route('/jobs/<job_id>/start', methods=['POST'])
def start_job_route(job_id):
    conn = connect_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    repo = PostgreSQLJobRepository(conn)
    scheduler = APSchedulerAdapter()
    use_case = SendNotificationUseCase(RabbitMQ())
    job_manager = JobManager(scheduler,use_case, repo)
    response = job_manager.start_job(job_id)
    conn.close()
    return jsonify(response), 200 if "message" in response else 400

@app.route('/jobs/<job_id>/stop', methods=['POST'])
def stop_job_route(job_id):
    conn = connect_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500