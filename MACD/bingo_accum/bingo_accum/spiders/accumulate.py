import scrapy
import pdb

from ..items import BingoAccumItem


class AccumulateSpider(scrapy.Spider):
    name = 'accumulate'
    allowed_domains = ['www.twse.com.tw']
    start_urls = ['file:///C:/Programs/bingo/MACD/bingo_ball.csv']
    ballTime = {}

    def __init__(self):
        for i in range(1, 81):
            self.ballTime[str(i).zfill(2)] = 0
        # pdb.set_trace()
        pass

    def parse(self, response):
        results = response.text.split('\r\n')
        for index, row in enumerate(results):
            # pdb.set_trace()
            if index == 0:  # Skip header rows
                continue
            arr = row.split(',')
            item = BingoAccumItem()
            item['DrawTerm'] = arr[0].replace('"', '')
            for i in range(1, 81):
                item['Ball' + str(i).zfill(2)] = self.ballTime[str(i).zfill(2)]
            # pdb.set_trace()
            if len(arr) != 23:
                continue
            for i in range(2, 22):
                # pdb.set_trace()
                self.ballTime[str(arr[i].replace('"', '')).zfill(2)] += 1
                # pdb.set_trace()
                item['Ball' + str(arr[i].replace('"', '')).zfill(2)
                     ] = self.ballTime[str(arr[i].replace('"', '')).zfill(2)]
            # pdb.set_trace()
            yield item
        pass
