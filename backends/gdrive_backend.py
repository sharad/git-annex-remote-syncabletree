# backends/gdrive_backend.py - GDrive backend for syncabletree using appProperties

import subprocess
import configparser
import io
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io as sysio

class GDriveBackend:
    def __init__(self, folder_id=None, rclone_remote_name="gdrive"):
        self.folder_id = folder_id
        self.creds = self.load_credentials_from_rclone(rclone_remote_name)
        self.service = build('drive', 'v3', credentials=self.creds)

    def load_credentials_from_rclone(self, remote_name):
        output = subprocess.check_output(["rclone", "config", "show"], text=True)
        config = configparser.ConfigParser()
        config.read_file(io.StringIO(output))

        section = config[remote_name]
        client_id = section.get("client_id")
        client_secret = section.get("client_secret")
        token_json = json.loads(section.get("token"))

        creds = Credentials(
            token=token_json["access_token"],
            refresh_token=token_json["refresh_token"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret
        )
        return creds

    def _search_file_by_name(self, name):
        query = f"name='{name}' and trashed=false"
        if self.folder_id:
            query += f" and '{self.folder_id}' in parents"
        results = self.service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, appProperties)'
        ).execute()
        files = results.get('files', [])
        return files[0] if files else None

    def _search_file_by_key(self, key):
        query = f"appProperties has {{ key='git-annex-key' and value='{key}' }} and trashed=false"
        if self.folder_id:
            query += f" and '{self.folder_id}' in parents"
        results = self.service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, appProperties)'
        ).execute()
        files = results.get('files', [])
        return files[0] if files else None

    def upload_file(self, file_path, readable_path, key):
        existing = self._search_file_by_name(readable_path)
        media = MediaFileUpload(file_path, resumable=True)
        file_metadata = {
            "name": readable_path,
            "appProperties": {
                "git-annex-key": key
            }
        }
        if self.folder_id:
            file_metadata["parents"] = [self.folder_id]

        if existing:
            self.service.files().update(
                fileId=existing["id"],
                body=file_metadata,
                media_body=media
            ).execute()
        else:
            self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id"
            ).execute()

    def download_file(self, key, dest_path):
        file = self._search_file_by_key(key)
        if not file:
            raise FileNotFoundError(f"Key {key} not found on GDrive.")

        request = self.service.files().get_media(fileId=file["id"])
        fh = sysio.FileIO(dest_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()

    def has_key(self, key):
        return self._search_file_by_key(key) is not None

    def remove_key(self, key):
        file = self._search_file_by_key(key)
        if file:
            self.service.files().delete(fileId=file["id"]).execute()

    def list_all_files_with_keys(self):
        query = "trashed=false"
        if self.folder_id:
            query += f" and '{self.folder_id}' in parents"

        page_token = None
        files_with_keys = {}
        while True:
            response = self.service.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name, appProperties)',
                pageToken=page_token
            ).execute()

            for file in response.get('files', []):
                key = file.get("appProperties", {}).get("git-annex-key")
                name = file["name"]
                if key:
                    files_with_keys[key] = name

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

        return files_with_keys
