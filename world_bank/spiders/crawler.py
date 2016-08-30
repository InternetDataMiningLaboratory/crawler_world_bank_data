# -*- coding: utf-8 -*-
#
# Author: Jimin Huang
#
'''
    爬取世界银行zip数据
'''
import scrapy
import logging
import arrow
import json
from scrapy.loader.processors import TakeFirst
from world_bank.items import WorldBankDataItem


logger = logging.getLogger(__name__)


class WorldBankDataSpider(scrapy.spiders.Spider):
    '''
        爬虫增量爬取世界银行zip数据
    '''
    # 爬虫名字
    name = 'world_bank_data'

    # 允许访问的网站域
    allowed_domains = [
        "datacatalog.worldbank.org/",
        "search.worldbank.org",
    ]

    # 开始爬取构造请求的网址
    start_urls = [
        (
            "http://search.worldbank.org/api/v2/dec?"
            "rows=500&fct=*&srt=Popularity&order=desc&format=json&"
            "fl=API%2CAPIAccessURL%2CDescription%2CType%2CURL%2C"
            "DetailPageURL%2CAcronym%2CLastRevisionDate%2CPeriodicit"
            "%2CPeriodicity_code"
            "%2CName%2CBulkDownload%2Cid%2CisRecentlyUpdated"
            "%2CisNew%2CMobileApp&"
            "apilang=En"
        ),
    ]

    def generate_data_item(self, data, public_time):
        '''
            根据传入data的值生成下载数据文件的请求
        '''
        item_loader = scrapy.loader.ItemLoader(item=WorldBankDataItem())
        item_loader.default_output_processor = TakeFirst()
        data_properties = data.split('=')
        if not data_properties[0]:
            return None
        item_loader.add_value(
            'name',
            data_properties[0]
        )
        item_loader.add_value(
            'url',
            data_properties[1]
        )
        item_loader.add_value(
            'public_time',
            public_time
        )
        try:
            filetype = data_properties[2]
        except IndexError:
            filetype = data_properties[1].split(u'.')[-1]
        item_loader.add_value(
            'filetype',
            filetype 
        )
        return item_loader.load_item()

    def parse(self, response):
        '''
            解析列表页的response，生成访问数据页面的请求
        '''
        # 解析response(json)得到数据对应超链接
        datum = json.loads(response.body)
        datum = datum['datacatalog']
        for data in datum:
            try:
                bulk_download = data[u'BulkDownload']
            except KeyError:
                continue
            try:
                public_time = data['LastRevisionDate_dt'].split('T')[0]
            except KeyError:
                public_time = u''

            for data_file in bulk_download.split(u';'):
                yield self.generate_data_item(data_file, public_time)