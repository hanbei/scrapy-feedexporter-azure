from scrapy.crawler import CrawlerRunner
from scrapy.spiders import Spider
from scrapy.utils.log import configure_logging
from twisted.internet import reactor


class BorisSpider(Spider):
    name = 'boris'
    start_urls = [
        'http://quotes.toscrape.com/page/1/',
        'http://quotes.toscrape.com/page/2/',
        'http://quotes.toscrape.com/page/3/',
        'http://quotes.toscrape.com/page/4/',
        'http://quotes.toscrape.com/page/5/',
    ]

    def parse(self, response, **kwargs):
        for quote in response.css('div.quote'):
            yield {
                'text': quote.css('span.text::text').get(),
                'author': quote.css('small.author::text').get(),
                'tags': quote.css('div.tags a.tag::text').getall(),
            }

    @staticmethod
    def close(spider, reason):
        return super().close(spider, reason)


configure_logging()
runner = CrawlerRunner(settings={
    'FEED_URI_PARAMS': 'scrapy-feedexporter-azure.storage.uri_params',
    'FEED_STORAGES': {
        "azblob": 'scrapy-feedexporter-azure.storage.BlobStorageFeedStorage',
        "azadls": 'scrapy-feedexporter-azure.storage.ADLSFeedStorage'
    },
    'FEEDS': {
        # How should the URLs be structured???ß
        # 'azblob://<account>/<container>/<blob_name>
        'azblob://devstoreaccount1.blob.core.windows.net/%(spider_name)s/%(year)s/%(month)s/%(day)s-%(batch_id)s.jl': {
            'format': 'jsonlines',
            'batch_item_count': 20
        }
    },
    'AZ_BLOB_CONNECTION_STRING': "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
})

d = runner.crawl(BorisSpider)
d.addBoth(lambda _: reactor.stop())
reactor.run()  # the script will block here until the crawling is finished
