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
        '''
            重载方法，对传入的每一个item进行处理
        '''
        # 使用item填充数据持久化对象，并插入到数据库中
        # 如果public_time未出现在item中，则直接置None
        data = Data(
            scrape_time=arrow.now().format('YYYY-MM-DD'),
            public_time=item['public_time']
            if 'public_time' in item.keys() else None,
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
        '''
            重载函数，生成下载文件的链接
            直接使用item的url属性作为下载使用的链接
        '''
        yield scrapy.Request(
            item['url']
        )

    def item_completed(self, results, item, info):
        '''
            重载函数，下载文件完成后的操作
        '''
        # 获取下载的状态作为item的status属性
        item['status'] = 'success' if results[0][0] else 'failed'

        # 获取下载的路径作为item的filepath属性
        # 如果status为failed则为空
        item['filepath'] = results[0][1]['path'] \
            if item['status'] == 'success' else None

        # 获取下载文件的校验和作为item的checksum属性
        # 如果status为failed则为空
        item['checksum'] = results[0][1]['checksum'] \
            if item['status'] == 'success' else None
        return item
