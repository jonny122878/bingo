# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pdb
from itemadapter import ItemAdapter


class PsBingoPipeline:
    def process_item(self, item, spider):
        item['strBigShowOrders'] = list(
            map(lambda x: str(x).zfill(2), item['bigShowOrder'].split(',')))
        pdb.set_trace()  # Set a breakpoint here
        return item
