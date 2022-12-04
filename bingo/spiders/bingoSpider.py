import scrapy


class BingospiderSpider(scrapy.Spider):
    name = 'bingoSpider'
    allowed_domains = ['www.taiwanlottery.com.tw']
    start_urls = ['http://www.taiwanlottery.com.tw/']

    def parse(self, response):
        pass
