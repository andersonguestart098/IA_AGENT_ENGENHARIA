import asyncio
import logging
from app.services.job_store import set_status

log = logging.getLogger("worker.demo")

async def run_demo_job(job_id: str, seconds: int = 5) -> None:
    await set_status(job_id, "running")
    log.info(f"Job {job_id} running for {seconds}s")
    try:
        await asyncio.sleep(seconds)
        await set_status(job_id, "done", result={"message": f"Finished after {seconds}s"})
    except Exception as e:
        await set_status(job_id, "failed", error=str(e))
        log.exception("Job failed")