# -*- coding: utf-8 -*-
#
# Author: QianfuFinance
#
'''
    测试 ``email_sender.py``
'''
from world_bank.email_sender import EmailSender
from nose.tools import assert_equals
from world_bank.config import Config


def test_EmailSender_from_config():
    '''
        UnitTest ``email_sender.EmailSender.from_config``
    '''
    class Config(object):
        def __init__(self):
            self.host = 'test'
            self.port = 1
            self.user = 'test'
            self.password = 'test'
            self.tls = True
            self.ssl = False
            self.from_addr = 'test'
    values = {
        'smtphost': 'test',
        'smtpport': 1,
        'smtpuser': 'test',
        'smtppass': 'test',
        'smtptls': True,
        'smtpssl': False,
        'mailfrom': 'test',
    }
    sender = EmailSender.from_config(Config())
    for key, value in values.iteritems():
        assert_equals(
            getattr(sender, key),
            value
        )


def test_EmailSender_send_info_mail():
    '''
        Test ``email_sender.EmailSender.send_info_mail``
    '''
    config = Config('test_email.yml')
    sender = EmailSender.from_config(config.email)
    sender.send_info_mail(config.info_email, 'test', 'test')