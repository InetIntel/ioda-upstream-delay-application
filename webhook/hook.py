from fastapi import FastAPI
import subprocess
import logging

app = FastAPI()

def sync_most_responsive_ip():
    # TODO: rsync
    # TODO: security check
    logging.info("sync_most_responsive_ip executed")


@app.post("/webhook/")
async def receive_webhook(request: dict):
    logging.info(f"Received request - {request}")
    sync_most_responsive_ip()
    return {
        "server_id": "1",
        "status": "Synchronization triggered"
    }
    