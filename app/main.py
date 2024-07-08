import logging

from fastapi import FastAPI, BackgroundTasks

from app.api.routes import router

log = logging.getLogger(__name__)

app = FastAPI()

app.include_router(router)

