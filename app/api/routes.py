from fastapi import APIRouter, BackgroundTasks

from app.worker.celery_worker import download_file

router = APIRouter()

@router.post("/download/")
async def download_endpoint(url: str):
    task = download_file.delay(url)
    return {"message": "Download task added to the queue", "task_id": task.id}