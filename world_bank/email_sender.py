# -*- coding: utf-8 -*-
#
# Author: QianfuFinance
#
'''
    邮件发送类，使用scrapy.mail
'''
from scrapy.mail import MailSender


class EmailSender(MailSender):
    @classmethod
    def from_config(cls, config):
        return cls(
            config.host,
            config.from_addr,
            config.user,
            config.password,
            config.port,
            config.tls,
            config.ssl,
        )

    def send_info_mail(self, config, subject, body):
        self.send(
            config.to,
            subject,
            body,
            config.cc,
            mimetype=config.mimetype,
        )
