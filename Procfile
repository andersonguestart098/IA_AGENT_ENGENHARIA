web: uvicorn app.api.main:app --host 0.0.0.0 --port $PORT
worker: arq app.workers.arq_worker.WorkerSettings