from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
from scrapy.extensions.feedexport import BlockingFeedStorage


class ADLSFeedStorage(BlockingFeedStorage):
    def __init__(self, uri: str, container: str, credentials: str):

        self.service = DataLakeServiceClient.from_connection_string(
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
        self.file_system_client = self.service.get_file_system_client(file_system="my-file-system")
        self.uri = uri  # TODO parse uri
        self.container = container
        self.credentials = credentials

    def _store_in_thread(self, file):
        file.seek(0)
        try:
            directory_client = self.file_system_client.get_directory_client("my-directory")
            file_client = directory_client.create_file("upload")
            f = file_client.create_file("test.txt")

            data = file.read()

            f.append_data(data=data, offset=0, length=len(data))

            f.flush_data(len(data))
        except Exception as e:
            print(e)

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):
        settings = crawler.settings
        storage_container = settings.get('ADLS_STORAGE_CONTAINER')
        storage_key = settings.get('ADLS_STORAGE_KEY')
        return cls(uri, storage_container, storage_key)


class BlobStorageFeedStorage(BlockingFeedStorage):
    def __init__(self, uri: str, container: str, credentials: str):

        self.blob_service = BlobServiceClient.from_connection_string(
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
        self.uri = uri  # TODO parse uri
        self.container = container
        self.credentials = credentials

    def _store_in_thread(self, file):
        file.seek(0)
        try:
            container = self.blob_service.get_container_client("test-container")
            if not container.exists():
                container.create_container("test-container")

            blob = container.upload_blob(name="test.txt", data=file)
        except Exception as e:
            print(e)

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):
        settings = crawler.settings
        storage_container = settings.get('ADLS_STORAGE_CONTAINER')
        storage_key = settings.get('ADLS_STORAGE_KEY')
        return cls(uri, storage_container, storage_key)
