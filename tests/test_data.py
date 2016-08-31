# -*- coding: utf-8 -*-
#
# Author: QianfuFinance
#
'''
    Test ``models.data``
'''
from world_bank.models.data import Data
from world_bank.database import DBSession
from test_database import TestService
from nose.tools import assert_equals
import arrow


class TestData(TestService):
    '''
        Test class of ``models.data``
    '''
    def setUp(self):
        super(TestData, self).setUp()
        with DBSession() as session:
            results = session.query(
                Data
            ).filter(Data.name == 'insert').all()
            map(session.delete, results)

    def tearDown(self):
        with DBSession() as session:
            results = session.query(
                Data
            ).filter(Data.name == 'insert').all()
            map(session.delete, results)

    def test_insert(self):
        '''
            UnitTest ``models.Data.insert``
        '''
        # 正常插入一条记录
        value_dict = {
            'scrape_time': '2016-08-10',
            'public_time': '2016-08-10',
            'source': 'test',
            'name': 'insert',
            'checksum': 'test',
            'filetype': 'test',
            'url': 'url',
            'status': 'test',
            'filepath': 'test',
        }
        obj = Data(**value_dict)
        Data.insert(obj)
        with DBSession() as session:
            results = session.query(
                Data
            ).filter(Data.name == 'insert').all()
            assert_equals(len(results), 1)
            result = results.pop()
            for key, value in value_dict.iteritems():
                real_value = getattr(result, key)
                if 'time' in key:
                    real_value = arrow.get(real_value).format('YYYY-MM-DD')
                assert_equals(
                    real_value,
                    value,
                )

        # 插入记录存在, checksum未改变
        value_dict = {
            'scrape_time': '2016-08-10',
            'public_time': '2016-08-10',
            'source': 'test',
            'name': 'insert',
            'checksum': 'test',
            'filetype': 'test',
            'url': 'url',
            'status': 'test',
            'filepath': 'test',
        }
        obj = Data(**value_dict)
        Data.insert(obj)
        with DBSession() as session:
            results = session.query(
                Data
            ).filter(Data.name == 'insert').all()
            assert_equals(len(results), 1)
            result = results.pop()
            for key, value in value_dict.iteritems():
                real_value = getattr(result, key)
                if 'time' in key:
                    real_value = arrow.get(real_value).format('YYYY-MM-DD')
                assert_equals(
                    real_value,
                    value,
                )

        # 插入记录存在, checksum已改变
        value_dict = {
            'scrape_time': '2016-09-10',
            'public_time': '2016-08-10',
            'source': 'test',
            'name': 'insert',
            'checksum': 'changed',
            'filetype': 'test',
            'url': 'url',
            'status': 'test',
            'filepath': 'test',
        }
        obj = Data(**value_dict)
        Data.insert(obj)
        with DBSession() as session:
            results = session.query(
                Data
            ).filter(Data.name == 'insert').all()
            assert_equals(len(results), 1)
            result = results.pop()
            for key, value in value_dict.iteritems():
                real_value = getattr(result, key)
                if 'time' in key:
                    real_value = arrow.get(real_value).format('YYYY-MM-DD')
                assert_equals(
                    real_value,
                    value,
                )
