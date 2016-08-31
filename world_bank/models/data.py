# -*- coding: utf-8 -*-
#
# Author: QianfuFinance
#
'''
    ``data`` 的持久化对象
'''
from world_bank.database import Base, DBBase, DBSession
from sqlalchemy import Column, String, DateTime
from sqlalchemy.exc import IntegrityError
import logging


logger = logging.getLogger(__name__)


class Data(Base, DBBase):
    '''
        表 ``data`` 的持久化对象
    '''
    __tablename__ = 'data'

    scrape_time = Column('scrape_time', DateTime)
    public_time = Column('public_time', DateTime)
    source = Column('source', String(255))
    name = Column('name', String(255))
    checksum = Column('checksum', String(255))
    filetype = Column('filetype', String(255))
    url = Column('url', String(255), primary_key=True)
    status = Column('status', String(255))
    filepath = Column('filepath', String(255))

    @classmethod
    def insert(cls, obj):
        '''
            插入对象
        '''
        with DBSession() as session:
            try:
                session.add(obj)
                session.flush()
            except IntegrityError, e:
                session.rollback()
                logger.info(e.message)
                if 'Duplicate' in e.message:
                    logger.info('Records exists')
                    record = session.query(cls).filter(
                        cls.url == obj.url
                    ).one()
                    if record.checksum != obj.checksum:
                        logger.info('Update data {0}'.format(record.url))
                        session.delete(record)
                        session.add(obj)
                        session.flush()
                    else:
                        logger.info('No update to {0}'.format(record.url))
                else:
                    logger.exception(e)
