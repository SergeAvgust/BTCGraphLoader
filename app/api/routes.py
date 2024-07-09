import re
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, HttpUrl, field_validator

from app.worker.celery_worker import download_file
from app.db.controller import db_driver
from app.db.query import ADDRESS_TRANSACTIONS_QUERY

router = APIRouter()

class URLOBJ(BaseModel):
    url: str

    @field_validator('url')
    def validate_custom_url(cls, v: HttpUrl) -> HttpUrl:
        pattern = re.compile(
            r'https://gz.blockchair.com/bitcoin/transactions/blockchair_bitcoin_(transactions|output|input)_\d{8}\.tsv\.gz'
        )
        if not pattern.match(str(v)):
            raise ValueError('URL does not match the required pattern')
        return v

@router.post("/download/")
async def download_endpoint(obj: URLOBJ):
    task = download_file.delay(obj.url)
    return {"message": "Download task added to the queue", "task_id": task.id}

@router.get("/transactions/{address}")
async def address_transactions_endpoint(address: str):
    with db_driver.session() as session:
        result = session.run(ADDRESS_TRANSACTIONS_QUERY, address=address)
        records = list(result)
        
        if not records or not records[0]['a']:
            raise HTTPException(status_code=404, detail="Address not found in the database")
        
        transactions = [record['t'] for record in records if record['t']]

    if not transactions:
        raise HTTPException(status_code=404, detail="No transactions found for this address")
    
    return transactions