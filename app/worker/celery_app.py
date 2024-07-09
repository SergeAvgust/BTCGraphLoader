import os

from celery import Celery
from celery.schedules import crontab
from redis import Redis


REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_USERNAME = os.getenv('REDIS_USERNAME', '')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', 'password')

"""
AMQP_HOST = os.getenv('AMQP_HOST', 'localhost')
AMQP_PORT = int(os.getenv('AMQP_PORT', '5672'))
AMQP_USERNAME = os.getenv('AMQP_USERNAME', 'user')
AMQP_PASSWORD = os.getenv('AMQP_PASSWORD', 'password')
"""

celery_app = Celery(
    'worker',
    broker='redis://%s:%s@%s:%d/0' % (
        REDIS_USERNAME,
        REDIS_PASSWORD,
        REDIS_HOST,
        REDIS_PORT
    ),
    backend='redis://%s:%s@%s:%d/0' % (
        REDIS_USERNAME,
        REDIS_PASSWORD,
        REDIS_HOST,
        REDIS_PORT
    )
)

celery_app.autodiscover_tasks()

celery_app.conf.update(
    task_track_started=True,
    beat_schedule={
        'add_loading_tasks_every_day': {
            'task': 'app.worker.celery_worker.schedule_daily_loading_tasks',
            'schedule': crontab(minute=0, hour=0),  # Run at midnight
        },
        'check_download_queue': {
            'task': 'app.worker.celery_worker.check_and_run_download_task',
            'schedule': crontab(minute='*')
        }
    },
    timezone='UTC',
)

redis_client = Redis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    db=0, 
    password=REDIS_PASSWORD, 
    username=REDIS_USERNAME
)
