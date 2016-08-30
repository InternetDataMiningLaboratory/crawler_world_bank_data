# -*- coding: utf-8 -*-
#
# Author: QianfuFinance
#
'''
    ``data`` 的持久化对象
'''
from world_bank.database import Base, DBBase, DBSession
from sqlalchemy import Column, String, Integer, DateTime
import logging


logger = logging.getLogger(__name__)


class Data(Base, DBBase):
    '''
        表 ``data`` 的持久化对象
    '''
    __tablename__ = 'data'

    data_id = Column('data_id', Integer, primary_key=True)
    scrape_time = Column('scrape_time', DateTime)
    public_time = Column('public_time', DateTime)
    source = Column('source', String(255))
    name = Column('name', String(255))
    filetype = Column('filetype', String(255))
    url = Column('url', String(255))
    status = Column('status', String(255))
    filepath = Column('filepath', String(255))

    @classmethod
    def insert(cls, obj):
        '''
            插入对象
        '''
        with DBSession() as session:
            session.add(obj)
            session.flush()
            id = obj.data_id
            return id

