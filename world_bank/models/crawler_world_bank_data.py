# -*- coding: utf-8 -*-
#
# Author: QianfuFinance
#
'''
    ``crawlerworldbankdata`` 的持久化对象
'''
from world_bank.database import Base, DBBase, DBSession
from sqlalchemy import Column, String, Integer, DateTime
import logging


logger = logging.getLogger(__name__)


class CrawlerWorldBankData(Base, DBBase):
    '''
        表 ``crawlerworldbankdata`` 的持久化对象
    '''
    __tablename__ = 'crawlerworldbankdata'

    instance_id = Column('instance_id', Integer, primary_key=True)
    start_time = Column('start_time', DateTime)
    finish_time = Column('finish_time', DateTime)
    item_scraped_count = Column('item_scraped_count', Integer)
    file_count = Column('file_count', Integer)
    file_status_count_uptodate = Column('file_status_count/uptodate', Integer)

    @classmethod
    def insert(cls, obj):
        '''
            插入对象
        '''
        with DBSession() as session:
            session.add(obj)
            session.flush()
