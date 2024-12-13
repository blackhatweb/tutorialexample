from flask import Flask, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
import uuid
import json
#import kết nối rabbitmq
from frameworks.rabbitmq import RabbitMQ
from use_cases.send_notification import SendNotificationUseCase

app = Flask(__name__)
scheduler = BackgroundScheduler()
scheduler.start()
jobs = {}
rabbitmq_connect = RabbitMQ()
send_notification_use_case = SendNotificationUseCase(rabbitmq_connect)

@app.route('/jobs', methods=['POST'])
def create_job():
    data = request.get_json()
    job_id = str(uuid.uuid4())
    try:
        scheduler.add_job(func=send_message_job, trigger='cron', id=job_id, kwargs = {'notification_data': data}, **parse_cron_string(data['schedule']))
    except ValueError as e:
         return jsonify({'error': str(e)}), 400
    jobs[job_id]= data
    jobs[job_id]['status'] = 'scheduled'
    return jsonify({'job_id': job_id}), 201

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
    try:
        scheduler.reschedule_job(job_id=job_id, trigger=None)
        jobs[job_id]['status'] = 'stopped'
        return jsonify({"message":"job stopped"}),200
    except Exception as e:
         return jsonify({'error': str(e)}), 400
@app.route('/jobs', methods=['GET'])
def get_all_jobs():
    return jsonify(list(jobs.values())),200

if __name__ == '__main__':
    app.run(debug=True)