from fastapi import APIRouter, HTTPException

from app.worker.celery_worker import download_file
from app.db.controller import db_driver
from app.db.query import ADDRESS_TRANSACTIONS_QUERY

router = APIRouter()

@router.post("/download/")
async def download_endpoint(url: str):
    task = download_file.delay(url)
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