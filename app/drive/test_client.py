import os
from app.drive.client import list_files_in_folder

if __name__ == "__main__":
    folder_id = os.getenv("GDRIVE_FOLDER_ID")
    files = list_files_in_folder(folder_id)
    for f in files[:20]:
        print(f["name"], "|", f["mimeType"])
    print("Total:", len(files))