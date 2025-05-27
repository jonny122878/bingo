# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
from collections import defaultdict
import pdb
from itemadapter import ItemAdapter
import json
import os
from typing import Dict, List


class ExportTimesBingoPipeline:

    def __init__(self):
        # self.data: Dict[str, List[int]] = defaultdict(list)
        pass

    def open_spider(self, spider):
        self.data = {}
        pass

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        # pdb.set_trace()
        # 將數據累積到字典中
        # self.data[adapter['ball']].append(adapter['times'])
        self.data[adapter['ball']] = adapter['times']
        return item

    def close_spider(self, spider):
        output_path = os.path.join(os.getcwd(), 'output.json')
        # pdb.set_trace()
        with open(output_path, 'w') as json_file:
            json.dump(self.data, json_file, indent=4)
