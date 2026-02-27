from dotenv import load_dotenv
import os

load_dotenv()

APP_ENV = os.getenv("APP_ENV", "local")

# Mongo
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

# Qdrant
QDRANT_URL = os.getenv("QDRANT_URL")

# Google Drive
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")