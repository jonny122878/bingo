import scrapy


class BingospiderSpider(scrapy.Spider):
    name = 'bingoSpider'
    allowed_domains = ['www.taiwanlottery.com.tw']
    start_urls = ['https://www.taiwanlottery.com.tw/lotto/bingobingo/drawing.aspx']

    def parse(self, response):
        # peroid = scrapy.Field()
        # num20 = scrapy.Field()

        item['peroid'] = response.selector.xpath('/html/body/form/div[3]/div[2]/div[3]/table[2]/tbody/tr/td/table/tbody/tr[4]/td[1]').getall()
        item['num20'] = response.selector.xpath('/html/body/form/div[3]/div[2]/div[3]/table[2]/tbody/tr/td/table/tbody/tr[4]/td[2]').getall()
        print('')
        pass
