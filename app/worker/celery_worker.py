import os
import requests
import gzip
import shutil
import os
import pandas as pd
from datetime import datetime, timedelta

from celery.utils.log import get_task_logger

from app.worker.celery_app import celery_app, redis_client
from app.worker.celery_mixins import BaseTaskWithRetry
from app.db.controller import db_driver
from app.db.query import create_input, create_or_update_btc_transaction, create_output

QUEUE_KEY = 'download_file_queue'
LOCK_KEY = 'download_file_lock'
LOCK_TIMEOUT = 10 * 60  # 10 min lock timeout

logger = get_task_logger(__name__)

@celery_app.task
def schedule_daily_loading_tasks():
    yesterday = datetime.now() - timedelta(days=2) # Seems new archives are posted not at UTC:23.59, so we will load previous one
    date_str = yesterday.strftime('%Y%m%d')
    
    urls = [
        f"https://gz.blockchair.com/bitcoin/transactions/blockchair_bitcoin_transactions_{date_str}.tsv.gz",
        f"https://gz.blockchair.com/bitcoin/inputs/blockchair_bitcoin_inputs_{date_str}.tsv.gz",
        f"https://gz.blockchair.com/bitcoin/outputs/blockchair_bitcoin_outputs_{date_str}.tsv.gz"
    ]
    for url in urls:
        download_file.delay(url)

@celery_app.task()
def download_file(url):
    """Enqueue file download"""
    redis_client.rpush(QUEUE_KEY, url)
    logger.info(f"Enqueued download task for URL: {url}")
    check_and_run_download_task.delay()


@celery_app.task(bind=True)
def check_and_run_download_task(self):
    if acquire_lock():
        try:
            url = redis_client.lpop(QUEUE_KEY)
            if url:
                process_file_download(url.decode('utf-8'))
        finally:
            release_lock()

def acquire_lock():
    return redis_client.set(LOCK_KEY, "locked", nx=True, ex=LOCK_TIMEOUT)

def release_lock():
    redis_client.delete(LOCK_KEY)


@celery_app.task(bind=True)
def process_file_download(self, url):
  
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 402:
            redis_client.rpush(QUEUE_KEY, url)
            return
        filename = url.split("/")[-1]

        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        result = f"{filename} downloaded"
        unzip_file.delay(filename)
        logger.info(f"File was successfully downloaded: {filename}")
    except Exception as e:
        logger.error(f"The following url download was not processed properly: {url} Exception: {e}")
    finally:
        redis_client.lrem(QUEUE_KEY, 0, self.request.id)
        if os.path.exists(filename):
            os.remove(filename)
    
    return result


@celery_app.task
def unzip_file(filename):
    try:
        if filename.endswith('.gz'):
            unzipped_filename = filename[:-3]
            with gzip.open(filename, 'rb') as f_in:
                with open(unzipped_filename, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(filename)  # Remove the .gz file after unzipping
            process_tsv.delay(unzipped_filename)
    except Exception as e:
        raise


@celery_app.task(bind=True, base=BaseTaskWithRetry)
def process_tsv(self, filename):
    tsv_code = None
    if 'transaction' in filename:
        tsv_code = 'transaction'
    elif 'input' in filename:
        tsv_code = 'input'
    elif 'output' in filename:
        tsv_code = 'output'
    
    map = {
        'transaction': create_or_update_btc_transaction,
        'input': create_input,
        'output': create_output
    }

    chunksize = 10000

    try:
        with db_driver.session() as session:
            for chunk in pd.read_csv(filename, sep='\t', chunksize=chunksize):
                rows = [row.to_dict() for _, row in chunk.iterrows()]
                session.write_transaction(map[tsv_code], rows)
    except Exception as e:
        self.retry(exc=e)
    finally:
        if os.path.exists(filename):
            os.remove(filename)

