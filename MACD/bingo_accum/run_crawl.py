from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from bingo_accum.spiders.accumulate import AccumulateSpider  # 替換為你的爬蟲名稱

if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl(AccumulateSpider)  # 替換為你的爬蟲名稱
    process.start()
