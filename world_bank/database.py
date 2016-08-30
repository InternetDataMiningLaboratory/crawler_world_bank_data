# -*- coding: utf-8 -*-
#
# Author: jimin.huang
#
'''
    数据库连接
'''
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from world_bank.config import Config


Base = declarative_base()
Settings = Config('config.yml').mysql 


def create_engine_with(
    user=Settings.user,
    password=Settings.password,
    host=Settings.host,
    port=Settings.port,
    database=Settings.database,
):
    '''
        使用参数创建engine，默认从配置文件读取
    '''
    engine = create_engine(
        'mysql+mysqldb://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(
            user,
            password,
            host,
            port,
            database,
        ),
        pool_recycle=3600,
        encoding='utf8',
    )
    return engine


_DBEngine = create_engine_with()
_DBMetadata = MetaData(bind=_DBEngine)
_Session = sessionmaker()


def init_db(engine=_DBEngine):
    '''
        初始化数据库及Session配置
    '''
    _Session.configure(bind=engine)


init_db()


class DBSession(object):
    '''
        封装Session类为更友好的with模式
    '''
    def __enter__(self):
        '''
            with语句返回一个数据库session
        '''
        self._session = _Session()
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        '''
            exit语句控制关闭后行为，如果有异常发生则回滚并抛出异常
        '''
        if exc_type is None:
            self._session.commit()
        else:
            self._session.rollback()
            self._session.close()
            raise exc_val
        self._session.close()


class DBBase(object):
    '''
        父类，加入get_items指向已注册的列及对应值
    '''
    def get_items(self):
        '''
            从Base类的 ``_sa_instance_state.attrs.items()`` 获取已注册的列及值
        '''
        return self._sa_instance_state.attrs.items()
