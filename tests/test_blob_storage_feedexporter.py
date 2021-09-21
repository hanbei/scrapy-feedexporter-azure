from io import StringIO

from pytest_mock import MockerFixture
from scrapy_feedexporter_azure.storage import BlobStorageFeedStorage


def test_saves_item(mocker: MockerFixture):
    blob_service = mocker.MagicMock()
    mocker.patch(
        'scrapy_feedexporter_azure.storage.BlobServiceClient').from_connection_string.return_value = blob_service

    container_client = mocker.MagicMock()
    blob_service.get_container_client.return_value = container_client

    feed_storage = BlobStorageFeedStorage("azblob://storage_account/container/path", "connection_string")
    file = StringIO('some text')
    feed_storage._store_in_thread(file)

    container_client.upload_blob.assert_called_with(name="path", data=file)
