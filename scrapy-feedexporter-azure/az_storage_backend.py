from scrapy.extensions.feedexport import BlockingFeedStorage


class ADLSFeedStorage(BlockingFeedStorage):
    def __init__(self, uri: str, container: str, credentials: str):
        self.uri = uri # TODO parse uri
        self.container = container
        self.credentials = credentials

    def _store_in_thread(self, file):
        file.seek(0)
        try:
            with open("test.txt", "wb") as f:
                while True:
                    data = file.read()
                    print(data)
                    if not data:
                        break
                    f.write(data)
                f.flush()
        except Exception as e:
            print(e)


    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):
        settings = crawler.settings
        storage_container = settings.get('ADLS_STORAGE_CONTAINER')
        storage_key = settings.get('ADLS_STORAGE_KEY')
        return cls(uri, storage_container, storage_key)
