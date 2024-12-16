# frameworks/postgresql_job_repository.py
import psycopg2
import uuid
from interfaces.job_repository import JobRepository
from entities.job import Job
from datetime import datetime, timezone

class PostgreSQLJobRepository(JobRepository):
    def __init__(self, conn): # Nhận kết nối từ bên ngoài
        self.conn = conn

    def create_job(self, job: Job) -> uuid.UUID:
        try:
            cur = self.conn.cursor()
            sql = """
                INSERT INTO jobs (id, name, status, schedule, message, target_audience, notification_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(sql, (job.id, job.name, job.status, job.schedule, job.message, job.target_audience, job.notification_type))
            self.conn.commit()
            return job.id
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Error inserting job: {e}")
            return None
        finally:
            cur.close()

    def get_job_by_id(self, job_id: uuid.UUID) -> Job:
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
            job_data = cur.fetchone()
            if job_data:
                return Job(*job_data)  # Tạo đối tượng Job từ dữ liệu database
            return None
        except psycopg2.Error as e:
            print(f"Error getting job: {e}")
            return None
        finally:
            cur.close()

    def update_job(self, job: Job):
        try:
            cur = self.conn.cursor()
            sql = """
                UPDATE jobs SET status = %s, next_run = %s, last_run = %s, updated_at = %s WHERE id = %s
            """
            now = datetime.now(timezone.utc)
            cur.execute(sql, (job.status, job.next_run, job.last_run, now, job.id))
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Error updating job: {e}")
        finally:
            cur.close()
    
    def get_all_jobs(self) -> list[Job]:
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM jobs")
            jobs_data = cur.fetchall()
            jobs_list = []
            for job_data in jobs_data:
                jobs_list.append(Job(*job_data))
            return jobs_list
        except psycopg2.Error as e:
            print(f"Error getting jobs: {e}")
            return None
        finally:
            cur.close()