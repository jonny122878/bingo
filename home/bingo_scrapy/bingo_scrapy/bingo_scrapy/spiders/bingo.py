import scrapy
import json
from datetime import datetime, timedelta
import requests
import logging
import os
class bingoSpider(scrapy.Spider):
    name = 'bingo_spider'

    def start_requests(self):
        try:
            if self.settings.getbool('USE_CRAWL_DATE'):
                dates = self.get_dates_crawl()
                logging.info(f'Using CRAWL_DATE: {dates}')
            else:
                dates = self.get_dates_interval()
                logging.info(f'Using START_DATE and END_DATE: {dates}')
            for date in dates:
                logging.info(f"爬取日期: {date}")
                print(f"爬取日期: {date}")
                url = f'https://api.taiwanlottery.com/TLCAPIWeB/Lottery/BingoResult?openDate={date}&pageNum=1&pageSize=1'
                yield scrapy.Request(url=url, meta={"date":date}, callback=self.parse)
        except Exception as e:
            logging.error(f'Error in start_requests: {e}')

    def get_dates_interval(self):
        try:
            logging.info(self.settings.get('START_DATE'))
            start_date = datetime.strptime(os.getenv('START_DATE'), '%Y-%m-%d')
            logging.info(start_date)
            end_date = datetime.strptime(os.getenv('END_DATE'), '%Y-%m-%d')
            logging.info(f'Start date: {start_date}, End date: {end_date}')
            dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
            return [date.strftime('%Y-%m-%d') for date in dates]
        except Exception as e:
            logging.error(f'Error in get_dates_interval: {e}')
            return []

    def get_dates_crawl(self):
        try:
            crawl_date = [datetime.now().strftime('%Y-%m-%d')]
            return crawl_date
        except Exception as e:
            logging.error(f'Error in get_dates_crawl: {e}')
            return []

    def parse(self, response):
        try:
            logging.info(f'Processing page: {response.url}')
            data = json.loads(response.text)

            total_size = data.get('content', {}).get('totalSize', 0)
            page_size = 10
            current_page = 0

            item = {
                'date': response.meta['date'],
                'items': []
            }
            for current_page in range(1, (total_size // page_size) + 2):
                current_page_url = f'https://api.taiwanlottery.com/TLCAPIWeB/Lottery/BingoResult?openDate={response.url.split("openDate=")[1].split("&")[0]}&pageNum={current_page}&pageSize={page_size}'
                response = requests.get(url=current_page_url)

                data = json.loads(response.text)
                results = data.get('content', {}).get('bingoQueryResult', [])

                for result in results:
                    sub_item = {
                        'drawTerm': result.get('drawTerm'),
                        'dDate': item['date'],
                        'bigShowOrder': result.get('bigShowOrder')
                    }
                    item['items'].append(sub_item)
            logging.info(f'Finished processing page: {response.url}')
            yield item
        except Exception as e:
            logging.error(f'Error in parse: {e}')
