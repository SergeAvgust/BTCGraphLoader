import os
import requests
import gzip
import shutil
import os
import pandas as pd

from app.worker.celery_app import celery_app, redis_client
from app.worker.celery_mixins import BaseTaskWithRetry
from app.db.controller import db_driver
from app.db.query import create_input, create_or_update_btc_transaction, create_output

QUEUE_KEY = 'download_queue'


@celery_app.task(bind=True)
def download_file(self, url):
    # Add the task ID to the Redis list
    redis_client.rpush(QUEUE_KEY, self.request.id)
    
    try:
        response = requests.get(url, stream=True)
        filename = url.split("/")[-1]

        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        result = f"{filename} downloaded"
        unzip_file.delay(filename)  # Chain the next task
    finally:
        # Remove the task ID from the Redis list
        redis_client.lrem(QUEUE_KEY, 0, self.request.id)
    
    return result


@celery_app.task
def unzip_file(filename):
    if filename.endswith('.gz'):
        unzipped_filename = filename[:-3]
        with gzip.open(filename, 'rb') as f_in:
            with open(unzipped_filename, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(filename)  # Remove the .gz file after unzipping
        process_tsv.delay(unzipped_filename)  # Chain the next task


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

