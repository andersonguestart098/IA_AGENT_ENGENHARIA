import os
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from dotenv import load_dotenv

load_dotenv()

_client: AsyncIOMotorClient | None = None

def connect_mongo() -> None:
    global _client
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI não configurado no .env")
    _client = AsyncIOMotorClient(mongo_uri)

def close_mongo() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None

def get_db() -> AsyncIOMotorDatabase:
    if _client is None:
        raise RuntimeError("Mongo client não inicializado. Chame connect_mongo() no startup.")
    db_name = os.getenv("MONGO_DB", "mvp_ia")
    return _client[db_name]