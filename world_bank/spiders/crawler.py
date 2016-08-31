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
        # 构造负责生成item的ItemLoader
        item_loader = scrapy.loader.ItemLoader(item=WorldBankDataItem())

        # 默认只输出第一个值
        item_loader.default_output_processor = TakeFirst()

        # 使用=对字符串进行切割
        data_properties = data.split('=')

        # 当为空字符串时，直接返回None
        if not data_properties[0]:
            return None

        # 将第一个值作为name存入
        item_loader.add_value(
            'name',
            data_properties[0]
        )

        # 将第二个值去除左右空格后作为url存入
        url = data_properties[1]
        url = url if '?' not in url else ''.join(url.split('?')[:-1])
        item_loader.add_value(
            'url',
            url.strip()
        )

        # 将传入的public_time存入
        item_loader.add_value(
            'public_time',
            public_time
        )

        # 将第三个值作为filetype存入
        # 如果没有第三个值，则用'.'切割下url的文件名后缀作为filetype存入
        try:
            filetype = data_properties[2]
        except IndexError:
            filetype = data_properties[1].split(u'.')[-1]
        item_loader.add_value(
            'filetype',
            filetype
        )

        # 返回构造的item
        return item_loader.load_item()

    def parse(self, response):
        '''
            解析列表页的response，生成访问数据页面的请求
        '''
        # 解析response(json)得到数据对应超链接
        datum = json.loads(response.body)

        # 首先获取解析好的json中的dastacatalog属性
        datum = datum['datacatalog']

        # 作为一个列表，遍历其中每一个数据
        for data in datum:
            # 尝试获取BulkDownload属性，这里一般存储文件下载链接
            # 如果没有这个属性，说明这个数据没有可以下载的文件
            try:
                bulk_download = data[u'BulkDownload']
            except KeyError:
                continue

            # 尝试从LastRevisionDate_dt中获取public_time
            # 使用'T'切割字符串得到'YYYY-MM-DD'形式的时间
            # 如果没有该属性，说明这个数据没有发布时间
            try:
                public_time = data['LastRevisionDate_dt'].split('T')[0]
            except KeyError:
                public_time = None

            # 对BulkDownload的值使用';'切割得到每一份下载文件的信息
            # 传入generate_data_item处理返回处理后的item
            for data_file in bulk_download.split(u';'):
                yield self.generate_data_item(data_file, public_time)