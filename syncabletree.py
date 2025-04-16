# syncabletree.py - Unified interface for syncable, human-readable annex remotes
import os

class SyncableTreeRemote:
    def __init__(self, backend):
        self.backend = backend

    def upload(self, file_path, annex_key):
        self.backend.upload_file(file_path, annex_key)

    def download(self, annex_key, dest_path):
        self.backend.download_file(annex_key, dest_path)

    def exists(self, annex_key):
        return self.backend.has_key(annex_key)

    def import_non_annexed_files(self, remote_dir, local_repo_dir, known_keys):
        """
        Detect and import files added directly to the remote directory (non-annexed).
        """
        imported = []
        for root, _, files in os.walk(remote_dir):
            for f in files:
                full_path = os.path.join(root, f)
                relative_path = os.path.relpath(full_path, remote_dir)
                if relative_path not in known_keys:
                    local_dest = os.path.join(local_repo_dir, relative_path)
                    os.makedirs(os.path.dirname(local_dest), exist_ok=True)
                    self.download(relative_path, local_dest)
                    imported.append(relative_path)
        return imported

