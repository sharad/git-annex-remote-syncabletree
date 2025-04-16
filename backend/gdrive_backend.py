# backends/gdrive_backend.py - Real Google Drive backend for git-annex custom remote
import os
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2.service_account import Credentials

class GDriveBackend:
    def __init__(self, credentials_json_path, drive_folder_id):
        self.folder_id = drive_folder_id
        scopes = ['https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(credentials_json_path, scopes=scopes)
        self.service = build('drive', 'v3', credentials=creds)

    def _search_file(self, name):
        query = f"name='{name}' and '{self.folder_id}' in parents and trashed=false"
        results = self.service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        items = results.get('files', [])
        return items[0] if items else None

    def upload_file(self, file_path, key):
        existing = self._search_file(key)
        file_metadata = {'name': key, 'parents': [self.folder_id]}
        media = MediaFileUpload(file_path, resumable=True)

        if existing:
            self.service.files().update(fileId=existing['id'], media_body=media).execute()
        else:
            self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()

    def download_file(self, key, dest_path):
        file_info = self._search_file(key)
        if not file_info:
            raise FileNotFoundError(f"Key {key} not found in Google Drive")

        request = self.service.files().get_media(fileId=file_info['id'])
        fh = io.FileIO(dest_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

    def has_key(self, key):
        return bool(self._search_file(key))
