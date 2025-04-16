# backends/gdrive.py - Real Google Drive backend for git-annex custom remote
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import os

class GDriveBackend:
    def __init__(self, credentials_path='credentials.json', settings_path='settings.yaml', folder_id=None):
        self.gauth = GoogleAuth(settings_file=settings_path)
        self.gauth.LoadCredentialsFile(credentials_path)
        if self.gauth.credentials is None:
            self.gauth.LocalWebserverAuth()
        elif self.gauth.access_token_expired:
            self.gauth.Refresh()
        else:
            self.gauth.Authorize()
        self.gauth.SaveCredentialsFile(credentials_path)

        self.drive = GoogleDrive(self.gauth)
        self.folder_id = folder_id

    def _get_file_by_key(self, key):
        query = f"title='{key}' and trashed=false"
        if self.folder_id:
            query += f" and '{self.folder_id}' in parents"
        file_list = self.drive.ListFile({'q': query}).GetList()
        return file_list[0] if file_list else None

    def upload_file(self, file_path, key):
        file_drive = self._get_file_by_key(key)
        if file_drive:
            file_drive.Delete()

        file_metadata = {
            'title': key,
            'parents': [{'id': self.folder_id}] if self.folder_id else []
        }
        file_drive = self.drive.CreateFile(file_metadata)
        file_drive.SetContentFile(file_path)
        file_drive.Upload()

    def download_file(self, key, dest_path):
        file_drive = self._get_file_by_key(key)
        if not file_drive:
            raise FileNotFoundError(f"Key {key} not found in Google Drive")
        file_drive.GetContentFile(dest_path)

    def has_key(self, key):
        return self._get_file_by_key(key) is not None
