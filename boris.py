from scrapy.crawler import CrawlerRunner
from scrapy.spiders import Spider
from scrapy.utils.log import configure_logging
from twisted.internet import reactor


class BorisSpider(Spider):
    name = 'boris'
    start_urls = [
        'http://quotes.toscrape.com/page/1/',
        'http://quotes.toscrape.com/page/2/',
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
    # 'EXTENSIONS': {'scrapy-feedexporter-adls.ADLSFeedStorage': 100},
    # 'FEED_STORAGES': {"az": 'scrapy-feedexporter-azure.storage.ADLSFeedStorage'},
    'FEED_STORAGES': {"az": 'scrapy-feedexporter-azure.storage.BlobStorageFeedStorage'},
    'FEEDS': {'az://storage_ccount.dfs.core.windows.net': {'format': 'csv'}},
    'ADLS_STORAGE_CONTAINER': 'storage_container',
    'ADLS_STORAGE_KEY': 'storage_key',
})

d = runner.crawl(BorisSpider)
d.addBoth(lambda _: reactor.stop())
reactor.run()  # the script will block here until the crawling is finished
