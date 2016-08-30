# -*- coding: utf-8 -*-
#
# Author: QianfuFinance
#
'''
    ``instance`` 的持久化对象
'''
from world_bank.database import Base, DBBase, DBSession
from sqlalchemy import Column, String, Integer
import logging


logger = logging.getLogger(__name__)


class Instance(Base, DBBase):
    '''
        表 ``instance`` 的持久化对象
    '''
    __tablename__ = 'instance'

    id = Column('id', Integer, primary_key=True)
    address = Column('address', String(255))
    name = Column('name', String(255))
    status = Column('status', String(255))
    module = Column('module', String(255))
    service = Column('service', String(255))

    @classmethod
    def insert(cls, obj):
        '''
            插入对象
        '''
        with DBSession() as session:
            session.add(obj)
            session.flush()
            id = obj.id
            return id

    @classmethod
    def update(cls, id, key, value):
        '''
            根据id更新对象
        '''
        with DBSession() as session:
            session.query(Instance).filter(
                Instance.id == id
            ).update({key: value})
