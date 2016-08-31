# -*- coding: utf-8 -*-
#
# Author: jimin.huang
#
import logging
import os
from scrapy import signals
from world_bank.models.instance import Instance
from world_bank.models.crawler_world_bank_data import CrawlerWorldBankData
from world_bank.config import Config
from world_bank.email_sender import EmailSender


logger = logging.getLogger(__name__)


class WorldBankExtension(object):
    '''
        用于创建及追踪爬虫状态的扩展
    '''
    def __init__(self, stats):
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        extension = cls(crawler.stats)
        crawler.signals.connect(
            extension.spider_opened,
            signal=signals.spider_opened
        )
        crawler.signals.connect(
            extension.spider_closed,
            signal=signals.spider_closed
        )
        return extension

    def spider_opened(self, spider):
        '''
            爬虫打开的时候，初始化记录项
        '''
        logger.info('Start logging crawler records')

    def spider_closed(self, spider, reason):
        '''
            爬虫关闭的时候，写入记录项到数据库
        '''
        records = CrawlerWorldBankData(
            instance_id=spider.instance_id,
            start_time=self.stats.get_value('start_time'),
            finish_time=self.stats.get_value('finish_time'),
            item_scraped_count=self.stats.get_value('item_scraped_count'),
            file_count=self.stats.get_value('file_count'),
            file_status_count_uptodate=self.stats.get_value('file_status_count/uptodate'),
        )
        CrawlerWorldBankData.insert(records)
        _config = Config('email.yml')
        email_sender = EmailSender.from_config(_config.email)
        email_sender.send_info_mail(
            _config.info_email,
            'Crawler {0} finished'.format(
                spider.name
            ),
            '\n'.join((
                'Stats:',
                'instance_id: {instance_id}',
                'start_time: {start_time}',
                'finish_time: {finish_time}',
                'item_scraped_count: {item_scraped_count}',
                'file_count: {file_count}',
                'file_status_count/uptodate: {file_status_count_uptodate}',
            )).format(
                instance_id=spider.instance_id,
                start_time=self.stats.get_value('start_time'),
                finish_time=self.stats.get_value('finish_time'),
                item_scraped_count=self.stats.get_value('item_scraped_count'),
                file_count=self.stats.get_value('file_count'),
                file_status_count_uptodate=self.stats.get_value('file_status_count/uptodate'),
            )
        )


class InstanceExtension(object):
    '''
        用于创建及追踪实例状态的扩展
    '''
    error_status = False

    @classmethod
    def from_crawler(cls, crawler):
        extension = cls()

        # Connect extensionension object to signals
        crawler.signals.connect(
            extension.spider_opened,
            signal=signals.spider_opened
        )
        crawler.signals.connect(
            extension.spider_closed,
            signal=signals.spider_closed
        )
        crawler.signals.connect(
            extension.spider_error,
            signal=signals.spider_error
        )
        return extension

    def spider_opened(self, spider):
        '''
            爬虫开启时，创建实例
        '''
        spider.instance_id = Instance.insert(Instance(
            name=os.environ['SCRAPY_JOB'],
            address='',
            service='world_bank',
            module='crawler',
            status='running',
        ))

    def spider_closed(self, spider, reason):
        '''
            爬虫关闭时，关闭实例
        '''
        if reason == 'finished' and not self.error_status:
            Instance.update(spider.instance_id, 'status', 'closed')
        else:
            Instance.update(spider.instance_id, 'status', 'error')

    def spider_error(self, failure, response, spider):
        '''
            爬虫发生错误，修改实例状态
        '''
        Instance.update(spider.instance_id, 'status', 'error')
        self.error_status = True
