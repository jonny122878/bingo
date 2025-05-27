# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from datetime import date
import scrapy


class TimesBingoItem(scrapy.Item):
    # define the fields for your item here like:
    drawTerm: int = scrapy.Field()
    dDate: date = scrapy.Field()
    bigShowOrder: str = scrapy.Field()
    createDate: str = scrapy.Field()
    """衍生"""
    bigShowOrders: list[int] = scrapy.Field()
    strBigShowOrders: list[str] = scrapy.Field()
    bigQty: int = scrapy.Field()
    smallQty: int = scrapy.Field()
    isBig: bool = scrapy.Field()
    isSmall: bool = scrapy.Field()


class PairTimesBingoItem(scrapy.Item):
    # define the fields for your item here like:
    ball: str = scrapy.Field()
    times: int = scrapy.Field()
