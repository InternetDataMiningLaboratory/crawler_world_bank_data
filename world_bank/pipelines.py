# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy
from scrapy.pipelines.files import FilesPipeline
from world_bank.models.data import Data
import arrow


class WorldBankPipeline(object):
    def process_item(self, item, spider):
        data = Data(
            scrape_time=arrow.now().format('YYYY-MM-DD'),
            public_time=item['public_time'],
            source='world_bank',
            filetype=item['filetype'],
            name=item['name'],
            url=item['url'],
            status=item['status'],
            filepath=item['filepath'],
        )
        Data.insert(data)
        return item


class WorldBankDownloadFilePipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        yield scrapy.Request(
            item['url']
        )

    def item_completed(self, results, item, info):
        item['status'] = 'success' if results[0][0] else 'failed'
        item['filepath'] = results[0][1]['path'] \
            if item['status'] == 'success' else None
        return item
