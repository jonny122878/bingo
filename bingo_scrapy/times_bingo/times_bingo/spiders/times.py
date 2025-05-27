import json
import pdb
import scrapy
from datetime import datetime

from times_bingo.items import PairTimesBingoItem, TimesBingoItem


class TimesSpider(scrapy.Spider):
    name = 'times'
    allowed_domains = []
    start_urls = ['file:///C:/Programs/bingo/test_data/ps/db_bingo.json']

    def parse(self, response):
        key_count = 80
        for key in range(1, key_count + 1):
            pairTimesBingoItem = PairTimesBingoItem()
            pairTimesBingoItem['ball'] = str(key).zfill(2)
            pairTimesBingoItem['times'] = 0
            # pdb.set_trace()
            yield pairTimesBingoItem
        # data = json.loads(response.text)
        # pdb.set_trace()  # Set a breakpoint here
        # timesBingoItem = TimesBingoItem()
        # # 次數累計
        # pairTimesBingoItem = PairTimesBingoItem()
        # for result in data:
        #     timesBingoItem['drawTerm'] = result['drawTerm']
        #     timesBingoItem['dDate'] = datetime.strptime(
        #         result['dDate'], '%Y-%m-%d').date()
        #     timesBingoItem['bigShowOrder'] = result['bigShowOrder']
        #     pdb.set_trace()
        #     yield timesBingoItem
        pass

# 先test 80 dict
# export df and json


# db.json
# 計算每日80號碼出現次數
# db.json * 30
# 80 * 30 = 2400 資料點
# 前1%做號碼統計次數


# 計算出30日中不分號碼，開出次數離群、中位數
