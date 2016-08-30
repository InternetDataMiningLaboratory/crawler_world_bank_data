# -*- coding: utf-8 -*-
#
# Author: QianfuFinance
#
'''
    Test ``models.crawler_world_bank_data``
'''
from world_bank.models.crawler_world_bank_data import CrawlerWorldBankData
from world_bank.database import DBSession
from test_database import TestService
from nose.tools import assert_equals
import arrow


class TestCrawlerWorldBankData(TestService):
    '''
        Test class of ``models.CrawlerWorldBankData``
    '''
    def setUp(self):
        super(TestCrawlerWorldBankData, self).setUp()
        with DBSession() as session:
            results = session.query(
                CrawlerWorldBankData
            ).filter(CrawlerWorldBankData.instance_id == -1).all()
            map(session.delete, results)

    def tearDown(self):
        with DBSession() as session:
            results = session.query(
                CrawlerWorldBankData
            ).filter(CrawlerWorldBankData.instance_id == -1).all()
            map(session.delete, results)

    def test_insert(self):
        '''
            UnitTest ``models.CrawlerWorldBankData.insert``
        '''
        value_dict = {
            'instance_id': -1,
            'start_time': '2016-08-09',
            'finish_time': '2016-08-09',
            'item_scraped_count': 1,
            'file_count': 1,
            'file_status_count_uptodate': 1,
        }
        obj = CrawlerWorldBankData(**value_dict)
        CrawlerWorldBankData.insert(obj)
        with DBSession() as session:
            results = session.query(
                CrawlerWorldBankData
            ).filter(CrawlerWorldBankData.instance_id == -1).all()
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
