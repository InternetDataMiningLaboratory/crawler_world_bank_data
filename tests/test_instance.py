# -*- coding: utf-8 -*-
#
# Author: QianfuFinance
#
'''
    Test ``models.instance``
'''
from world_bank.models.instance import Instance
from world_bank.database import DBSession
from test_database import TestService
from nose.tools import assert_equals


class TestInstance(TestService):
    '''
        Test class of ``models.Instance``
    '''
    def setUp(self):
        super(TestInstance, self).setUp()
        with DBSession() as session:
            results = session.query(
                Instance
            ).filter(Instance.status == 'insert').all()
            map(session.delete, results)

    def tearDown(self):
        with DBSession() as session:
            results = session.query(
                Instance
            ).filter(Instance.status == 'insert').all()
            map(session.delete, results)

        with DBSession() as session:
            results = session.query(
                Instance
            ).filter(Instance.status == 'updated').update({'status': 'updat'})

    def test_insert(self):
        '''
            UnitTest ``models.Instance.insert``
        '''
        value_dict = {
            'name': 'test',
            'address': 'test',
            'status': 'insert',
            'module': 'test',
            'service': 'test',
        }
        obj = Instance(**value_dict)
        value_dict['id'] = Instance.insert(obj)
        with DBSession() as session:
            results = session.query(
                Instance
            ).filter(Instance.status == 'insert').all()
            assert_equals(len(results), 1)
            result = results.pop()
            for key, value in value_dict.iteritems():
                assert_equals(
                    getattr(result, key),
                    value,
                )

    def test_udpate(self):
        '''
            UnitTest ``models.Instance.update``
        '''
        value_dict = {
            'id': 1,
            'name': 'test',
            'address': 'test',
            'status': 'updated',
            'module': 'test',
            'service': 'test',
        }
        Instance.update(1, 'status', 'updated')
        with DBSession() as session:
            results = session.query(
                Instance
            ).filter(Instance.status == 'updated').all()
            assert_equals(len(results), 1)
            result = results.pop()
            for key, value in value_dict.iteritems():
                assert_equals(
                    getattr(result, key),
                    value,
                )
