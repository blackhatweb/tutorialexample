from flask import Flask, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
import uuid
import json
#import kết nối rabbitmq
from frameworks.rabbitmq import RabbitMQ
from use_cases.send_notification import SendNotificationUseCase

import psycopg2
import uuid
import json
from datetime import datetime, timezone

# ... (các import khác)

def connect_db():
    try:
        conn = psycopg2.connect("dbname=your_db user=your_user password=your_password host=your_host port=your_port")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def create_job_in_db(conn, job_data):
    try:
        cur = conn.cursor()
        job_id = uuid.uuid4()
        sql = """
            INSERT INTO jobs (id, name, schedule, message, target_audience, notification_type)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        cur.execute(sql, (job_id, job_data['name'], job_data['schedule'], job_data['message'], job_data['target_audience'], job_data['notification_type']))
        conn.commit()
        return job_id
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error inserting job: {e}")
        return None
    finally:
        cur.close()

def update_job_status(conn, job_id, status, next_run=None):
    try:
        cur = conn.cursor()
        sql = """
        UPDATE jobs SET status = %s, next_run = %s, updated_at = %s WHERE id = %s
        """
        now = datetime.now(timezone.utc)
        cur.execute(sql,(status, next_run, now, job_id))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error updating job status: {e}")
    finally:
        cur.close()


def get_all_jobs_from_db(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs")
        jobs = cur.fetchall()
        # Chuyển đổi kết quả thành list of dict
        jobs_list = []
        column_names = [desc[0] for desc in cur.description]
        for job in jobs:
            jobs_list.append(dict(zip(column_names, job)))
        return jobs_list

    except psycopg2.Error as e:
        print(f"Error getting jobs: {e}")
        return None

    finally:
        cur.close()

###############

app = Flask(__name__)
scheduler = BackgroundScheduler()
scheduler.start()
jobs = {}
rabbitmq_connect = RabbitMQ()
send_notification_use_case = SendNotificationUseCase(rabbitmq_connect)

# Ví dụ sử dụng trong Flask route:
@app.route('/jobs', methods=['POST'])
def create_job():
    conn = connect_db()
    if conn is None:
        return jsonify({'error': 'Database connection failed'}), 500

    data = request.get_json()
    job_id = create_job_in_db(conn,data)
    conn.close()
    if job_id:
        try:
            scheduler.add_job(func=send_message_job, trigger='cron', id=job_id, kwargs = {'notification_data': data}, **parse_cron_string(data['schedule']))
        except ValueError as e:
            return jsonify({'error': str(e)}), 400
        return jsonify({'job_id': str(job_id)}), 201
    else:
         return jsonify({'error': 'Can not create job'}), 500

# @app.route('/jobs', methods=['POST'])
# def create_job():
#     data = request.get_json()
#     job_id = str(uuid.uuid4())
    
#     jobs[job_id]= data
#     jobs[job_id]['status'] = 'scheduled'
#     jobs[job_id]['job_id'] = job_id
    
#     return jsonify({'job_id': job_id}), 201

def parse_cron_string(cron_string):
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

def send_message_job(notification_data):
     response = send_notification_use_case.execute(notification_data)
     print(response)

#...
@app.route('/jobs/<job_id>/start', methods=['POST'])
def start_job(job_id):
    conn = connect_db()
    if conn is None:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
         update_job_status(conn, uuid.UUID(job_id), 'running')
         conn.close()
         return jsonify({'message': 'Job Started'}), 200
    except ValueError as e:
        conn.close()
        return jsonify({'error': str(e)}), 400

@app.route('/jobs/<job_id>/start', methods=['POST'])
def start_job(job_id):
    try:
        if scheduler.get_job(job_id).next_run is None:
            scheduler.reschedule_job(job_id=job_id, trigger='cron', **parse_cron_string(jobs[job_id]['schedule']))
        jobs[job_id]['status'] = 'running'
        return jsonify({"message":"job started"}),200
    except Exception as e:
         return jsonify({'error': str(e)}), 400


@app.route('/jobs/<job_id>/stop', methods=['POST'])
def stop_job(job_id):
    conn = connect_db()
    if conn is None:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
         update_job_status(conn, uuid.UUID(job_id), 'stopped')
         conn.close()
         return jsonify({'message': 'Job stopped'}), 200
    except ValueError as e:
        conn.close()
        return jsonify({'error': str(e)}), 400
    
@app.route('/jobs/<job_id>/stop', methods=['POST'])
def stop_job(job_id):
    try:
        scheduler.reschedule_job(job_id=job_id, trigger=None)
        jobs[job_id]['status'] = 'stopped'
        return jsonify({"message":"job stopped"}),200
    except Exception as e:
         return jsonify({'error': str(e)}), 400
    
@app.route('/jobs', methods=['GET'])
def get_all_jobs():
     conn = connect_db()
     if conn is None:
        return jsonify({'error': 'Database connection failed'}), 500
     jobs_list = get_all_jobs_from_db(conn)
     conn.close()
     return jsonify(jobs_list), 200

#... (code Flask và APScheduler khác)

@app.route('/jobs', methods=['GET'])
def get_all_jobs():
    return jsonify(list(jobs.values())),200

if __name__ == '__main__':
    app.run(debug=True)