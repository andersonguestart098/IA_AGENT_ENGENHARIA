web: uvicorn app.api.main:app --host 0.0.0.0 --port $PORT
worker: python -m app.workers.arq_worker