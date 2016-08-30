# -*- coding: utf-8 -*-
#
# Author: jimin.huang
#
'''
    数据库集成测试
'''
from world_bank.database import DBSession, init_db, create_engine_with
import nose.tools as tools
import pkg_resources
from mock import Mock, patch


@patch("world_bank.database._Session")
def test_DBSession(mock_Session):
    '''
        测试 ``database.DBSession``
    '''
    # 构建mock
    mock_session = Mock()
    mock_session.commit.return_value = None
    mock_session.rollback.return_value = None
    mock_session.close.return_value = None
    mock_Session.return_value = mock_session

    # 正常调用session
    with DBSession() as session:
        tools.assert_equal(session, mock_session)
        mock_Session.assert_called_with()
    mock_session.commit.assert_called_with()
    mock_session.close.assert_called_with()

    # with语句抛出异常
    def _nested_func():
        with DBSession():
            raise Exception()
    tools.assert_raises(Exception, _nested_func)
    mock_session.rollback.assert_called_with()
    mock_session.close.assert_called_with()


class TestService(object):
    '''
        测试服务方法类
    '''
    def setUp(self):
        '''
            测试初始化，使用测试配置文件创建测试数据库连接
        '''
        import yaml
        with open(
            pkg_resources.resource_filename(
                'world_bank.conf',
                'test_service_config.yml'
            )
        ) as ymlfile:
            _config = yaml.load(ymlfile)
        self.test_engine = create_engine_with(**_config)
        init_db(self.test_engine)
