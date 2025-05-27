import pdb
import unittest
from scrapy.http import TextResponse, Request

from bingo_accum.spiders.accumulate import AccumulateSpider


class TestAccumulateSpider(unittest.TestCase):
    def setUp(self):
        # 初始化爬蟲
        self.spider = AccumulateSpider()

    def test_parse(self):
        # 模擬 CSV 文件內容
        csv_content = (
            'drawTerm,dDate,bigShowOrder,createDate\r\n'
            '114018271,2025-04-01,"13,15,17,20,22,35,36,40,46,50,51,57,61,64,66,70,75,76,77,78",2025-04-03 15:43:48.773000\r\n'
            '114018272,2025-04-01,"04,06,07,08,12,13,14,18,25,29,36,48,49,51,54,64,67,75,78,80",2025-04-03 15:43:48.760000\r\n'
        )

        # 模擬 Scrapy 的 Response
        request = Request(url='file:///C:/Programs/bingo/MACD/bingo_ball.csv')
        response = TextResponse(
            url=request.url, body=csv_content, encoding='utf-8', request=request)

        # 測試 parse 方法
        results = list(self.spider.parse(response))
        # pdb.set_trace()
        # 驗證結果
        self.assertEqual(len(results), 2)  # 應該有兩行數據
        self.assertEqual(results[0]['DrawTerm'], '114018271')  # 期數驗證
        self.assertEqual(results[1]['DrawTerm'], '114018272')  # 期數驗證
        self.assertEqual(results[1]['Ball01'], 0)  # Ball01 累加無出現次數
        self.assertEqual(results[1]['Ball04'], 1)  # Ball13 累加出現次數
        self.assertEqual(results[1]['Ball13'], 2)  # Ball13 累加出現次數
        self.assertEqual(results[1]['Ball20'], 1)  # Ball20 累加出現次數


if __name__ == '__main__':
    unittest.main()
