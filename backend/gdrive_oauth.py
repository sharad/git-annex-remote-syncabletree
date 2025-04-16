# backends/gdrive_oauth.py - Google Drive backend using rclone-style config
import io
import configparser
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2.credentials import Credentials

class GDriveBackend:
    def __init__(self, config_path="gdrive_oauth.ini", section="gdrive", folder_id=None):
        config = configparser.ConfigParser()
        config.read(config_path)

        creds = Credentials(
            token=config[section].get("access_token", ""),  # optional
            refresh_token=config[section]["refresh_token"],
            token_uri=config[section].get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=config[section]["client_id"],
            client_secret=config[section]["client_secret"]
        )
        self.service = build("drive", "v3", credentials=creds)
        self.folder_id = folder_id

    def _search_file(self, name):
        query = f"name='{name}' and trashed=false"
        if self.folder_id:
            query += f" and '{self.folder_id}' in parents"
        results = self.service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        items = results.get('files', [])
        return items[0] if items else None

    def upload_file(self, file_path, key):
        existing = self._search_file(key)
        file_metadata = {'name': key}
        if self.folder_id:
            file_metadata['parents'] = [self.folder_id]

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
