import json
import pdb
import scrapy
from datetime import datetime

from times_bingo.items import TimesBingoItem


class TimesSpider(scrapy.Spider):
    name = 'times'
    allowed_domains = []
    start_urls = ['file:///C:/Programs/bingo/test_data/ps/db_bingo.json']

    def parse(self, response):
        data = json.loads(response.text)
        pdb.set_trace()  # Set a breakpoint here
        item = TimesBingoItem()
        for result in data:
            item['drawTerm'] = result['drawTerm']
            item['dDate'] = datetime.strptime(
                result['dDate'], '%Y-%m-%d').date()
            item['bigShowOrder'] = result['bigShowOrder']
            pdb.set_trace()
            yield item
        pass
