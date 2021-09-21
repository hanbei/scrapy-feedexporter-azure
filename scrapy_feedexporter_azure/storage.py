import datetime
from urllib.parse import urlparse

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
    def __init__(self, uri: str, connection_string: str = None):
        if connection_string:
            self.blob_service = BlobServiceClient.from_connection_string(connection_string)
        else:
            raise Exception

        self.uri = urlparse(uri)   # TODO parse uri
        self.account = self.uri[1]
        self.container = self.uri[2].split("/")[1]
        self.path = "/".join(self.uri[2].split("/")[2:])

    def _store_in_thread(self, file):
        file.seek(0)
        try:
            container_client = self.blob_service.get_container_client(self.container)
            if not container_client.exists():
                container_client.create_container()

            # TODO Use batch_id if set
            blob = container_client.upload_blob(name=self.path, data=file)
        except Exception as e:
            print(e)

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):
        settings = crawler.settings
        connection_string = settings.get('AZ_BLOB_CONNECTION_STRING')
        return cls(uri, connection_string)


def uri_params(params, spider):
    date = datetime.datetime.now()
    params.update({
        'spider_name': spider.name,
        'year': date.year,
        'month': date.month,
        'day': date.day,
        'hour': date.hour,
        'minute': date.minute,
    })
