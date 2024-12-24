import json
import pdb
import scrapy

from ps_bingo.items import PsBingoItem


class DeferSpider(scrapy.Spider):
    name = 'defer'
    allowed_domains = ['do.com']
    # 绝对路径：url = 'file:///c:/Projects/Article/spiders/start.html'
    # 绝对路径：url = 'file:c:\Projects\Article\spiders\start.html'
    # 相对路径：url = 'file:%s' % os.path.abspath('start.html')
    start_urls = ['file:///C:/Programs/bingo/test_data/ps/db_bingo.json']

    def parse(self, response):
        data = json.loads(response.text)
        pdb.set_trace()  # Set a breakpoint here
        item = PsBingoItem()
        for result in data:
            item['drawTerm'] = result['drawTerm']
            item['dDate'] = result['dDate']
            item['bigShowOrder'] = result['bigShowOrder']
            # pdb.set_trace()
            yield item
        pass
