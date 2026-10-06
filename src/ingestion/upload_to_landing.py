from pathlib import Path
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from src.common import config
import hashlib

def get_datalake_client():
    credential = ClientSecretCredential(
        tenant_id=config.AZURE_TENANT_ID,
        client_id=config.AZURE_CLIENT_ID,
        client_secret=config.AZURE_CLIENT_SECRET
    )

    account_url = (
        f"https://{config.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    )

    return DataLakeServiceClient(
        account_url=account_url,
        credential=credential
    )

def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def upload_file(
        local_path: Path,
        filesystem_name: str = "landing"
) -> None:
    client = get_datalake_client()

    filesystem_client = client.get_file_system_client(filesystem_name)

    with open(local_path, "rb") as file:
        data = file.read()

    file_client = filesystem_client.get_file_client(local_path.name)
    file_client.upload_data(
        data,
        overwrite=True
    )
    print(f"Uploaded: {local_path} -> {filesystem_name}/{local_path.name}")

    hash_str = compute_sha256(data)
    hash_file_name = local_path.name + ".sha256"
    hash_file_client = filesystem_client.get_file_client(hash_file_name)
    hash_file_client.upload_data(hash_str.encode(), overwrite=True)
    print(f"Uploaded: {hash_file_name} -> {filesystem_name}/{hash_file_name}")

if __name__ == "__main__":
    raw_dir = Path("data/raw")

    for csv_file in raw_dir.glob("*.csv"):
        upload_file(csv_file)

