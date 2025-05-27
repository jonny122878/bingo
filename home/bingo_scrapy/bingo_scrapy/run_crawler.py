# run_crawler.py
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

def main():
    process = CrawlerProcess(get_project_settings())
    process.crawl("bingo_spider")  # 這裡對應你的 spider 名稱
    process.start()

if __name__ == "__main__":
    main()
